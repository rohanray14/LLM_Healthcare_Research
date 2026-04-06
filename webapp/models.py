import sqlite3
from config import DATABASE_PATH


def get_db():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS posts (
            id TEXT PRIMARY KEY,
            title TEXT,
            body TEXT,
            label1 TEXT,
            label2 TEXT,
            label3 TEXT,
            num_comments INTEGER,
            reddit_url TEXT
        );

        CREATE TABLE IF NOT EXISTS comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id TEXT REFERENCES posts(id),
            comment_index INTEGER,
            text TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_comments_post ON comments(post_id);

        CREATE TABLE IF NOT EXISTS llm_outputs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id TEXT REFERENCES posts(id),
            model_family TEXT,
            model_name TEXT,
            summary TEXT,
            unique_advice_json TEXT,
            divergences_json TEXT,
            clinically_relevant_notes_json TEXT,
            data_quality TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_llm_post ON llm_outputs(post_id);

        CREATE TABLE IF NOT EXISTS label_verifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id TEXT REFERENCES posts(id),
            expert_username TEXT,
            label1_correct INTEGER,
            label2_correct INTEGER,
            label3_correct INTEGER,
            suggested_label TEXT,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(post_id, expert_username)
        );

        CREATE TABLE IF NOT EXISTS comment_verifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id TEXT REFERENCES posts(id),
            comment_index INTEGER,
            expert_username TEXT,
            flag TEXT CHECK(flag IN ('rumor', 'credible', 'unclear')),
            note TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(post_id, comment_index, expert_username)
        );
    """)
    conn.close()


def get_posts(label_filter=None, verified_filter=None, search=None, page=1, per_page=25, username=None):
    conn = get_db()
    conditions = []
    params = []

    if label_filter:
        conditions.append("(p.label1 = ? OR p.label2 = ? OR p.label3 = ?)")
        params.extend([label_filter, label_filter, label_filter])

    if search:
        conditions.append("(p.title LIKE ? OR p.id LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])

    where = ""
    if conditions:
        where = "WHERE " + " AND ".join(conditions)

    # Get total count
    count_sql = f"SELECT COUNT(*) FROM posts p {where}"
    total = conn.execute(count_sql, params).fetchone()[0]

    # Get posts with verification status
    sql = f"""
        SELECT p.*,
            (SELECT COUNT(DISTINCT expert_username) FROM label_verifications lv WHERE lv.post_id = p.id) as verifier_count,
            (SELECT COUNT(*) FROM comment_verifications cv WHERE cv.post_id = p.id AND cv.expert_username = ?) as my_flags
        FROM posts p
        {where}
    """
    full_params = [username or ""] + params

    if verified_filter == "verified":
        sql = f"""
            SELECT p.*,
                (SELECT COUNT(DISTINCT expert_username) FROM label_verifications lv WHERE lv.post_id = p.id) as verifier_count,
                (SELECT COUNT(*) FROM comment_verifications cv WHERE cv.post_id = p.id AND cv.expert_username = ?) as my_flags
            FROM posts p
            {where}
            {"AND" if where else "WHERE"} EXISTS (SELECT 1 FROM label_verifications lv WHERE lv.post_id = p.id)
        """
    elif verified_filter == "unverified":
        sql = f"""
            SELECT p.*,
                0 as verifier_count,
                0 as my_flags
            FROM posts p
            {where}
            {"AND" if where else "WHERE"} NOT EXISTS (SELECT 1 FROM label_verifications lv WHERE lv.post_id = p.id)
        """

    sql += f" ORDER BY p.num_comments DESC LIMIT ? OFFSET ?"
    full_params.extend([per_page, (page - 1) * per_page])

    rows = conn.execute(sql, full_params).fetchall()
    conn.close()
    return rows, total


def get_post(post_id):
    conn = get_db()
    post = conn.execute("SELECT * FROM posts WHERE id = ?", (post_id,)).fetchone()
    comments = conn.execute(
        "SELECT * FROM comments WHERE post_id = ? ORDER BY comment_index", (post_id,)
    ).fetchall()
    conn.close()
    return post, comments


def get_llm_outputs(post_id):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM llm_outputs WHERE post_id = ? ORDER BY model_family", (post_id,)
    ).fetchall()
    conn.close()
    return rows


def get_adjacent_posts(post_id, label_filter=None):
    conn = get_db()
    cond = ""
    params = []
    if label_filter:
        cond = "AND (label1 = ? OR label2 = ? OR label3 = ?)"
        params = [label_filter, label_filter, label_filter]

    prev_post = conn.execute(
        f"SELECT id FROM posts WHERE id < ? {cond} ORDER BY id DESC LIMIT 1",
        [post_id] + params,
    ).fetchone()

    next_post = conn.execute(
        f"SELECT id FROM posts WHERE id > ? {cond} ORDER BY id ASC LIMIT 1",
        [post_id] + params,
    ).fetchone()

    conn.close()
    return (prev_post["id"] if prev_post else None, next_post["id"] if next_post else None)


def get_existing_verifications(post_id, username):
    conn = get_db()
    label_v = conn.execute(
        "SELECT * FROM label_verifications WHERE post_id = ? AND expert_username = ?",
        (post_id, username),
    ).fetchone()
    comment_vs = conn.execute(
        "SELECT * FROM comment_verifications WHERE post_id = ? AND expert_username = ? ORDER BY comment_index",
        (post_id, username),
    ).fetchall()
    conn.close()
    return label_v, {cv["comment_index"]: cv for cv in comment_vs}


def save_verification(post_id, username, label_data, comment_flags):
    conn = get_db()

    conn.execute(
        """INSERT INTO label_verifications
            (post_id, expert_username, label1_correct, label2_correct, label3_correct, suggested_label, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(post_id, expert_username) DO UPDATE SET
            label1_correct=excluded.label1_correct,
            label2_correct=excluded.label2_correct,
            label3_correct=excluded.label3_correct,
            suggested_label=excluded.suggested_label,
            notes=excluded.notes,
            created_at=CURRENT_TIMESTAMP
        """,
        (
            post_id,
            username,
            label_data.get("label1_correct", 0),
            label_data.get("label2_correct", 0),
            label_data.get("label3_correct", 0),
            label_data.get("suggested_label", ""),
            label_data.get("notes", ""),
        ),
    )

    for idx, flag_data in comment_flags.items():
        if flag_data["flag"]:
            conn.execute(
                """INSERT INTO comment_verifications
                    (post_id, comment_index, expert_username, flag, note)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(post_id, comment_index, expert_username) DO UPDATE SET
                    flag=excluded.flag, note=excluded.note, created_at=CURRENT_TIMESTAMP
                """,
                (post_id, idx, username, flag_data["flag"], flag_data.get("note", "")),
            )

    conn.commit()
    conn.close()


def get_progress():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    verified = conn.execute(
        "SELECT COUNT(DISTINCT post_id) FROM label_verifications"
    ).fetchone()[0]
    comments_flagged = conn.execute(
        "SELECT COUNT(*) FROM comment_verifications"
    ).fetchone()[0]
    conn.close()
    return {"total": total, "verified": verified, "comments_flagged": comments_flagged}


def posts_loaded():
    conn = get_db()
    try:
        count = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
        conn.close()
        return count > 0
    except Exception:
        conn.close()
        return False
