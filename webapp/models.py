import sqlite3
from werkzeug.security import generate_password_hash, check_password_hash
from config import DATABASE_PATH, DATABASE_URL, USE_POSTGRES

if USE_POSTGRES:
    import psycopg2
    import psycopg2.extras


class DBConnection:
    """Thin wrapper so the rest of the code works with both SQLite and PostgreSQL."""

    def __init__(self, conn, is_pg):
        self._conn = conn
        self._is_pg = is_pg

    def execute(self, sql, params=None):
        cur = self._conn.cursor()
        if self._is_pg:
            sql = sql.replace("?", "%s")
        cur.execute(sql, params or ())
        return cur

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()


def get_db():
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL)
        conn.cursor_factory = psycopg2.extras.RealDictCursor
        return DBConnection(conn, is_pg=True)
    else:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return DBConnection(conn, is_pg=False)


def init_db():
    conn = get_db()

    tables = [
        """CREATE TABLE IF NOT EXISTS posts (
            id TEXT PRIMARY KEY,
            title TEXT,
            body TEXT,
            label1 TEXT,
            label2 TEXT,
            label3 TEXT,
            num_comments INTEGER,
            reddit_url TEXT,
            assigned_batch TEXT,
            assigned_annotator TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS comments (
            id {serial} PRIMARY KEY,
            post_id TEXT REFERENCES posts(id),
            comment_index INTEGER,
            text TEXT
        )""",
        "CREATE INDEX IF NOT EXISTS idx_comments_post ON comments(post_id)",
        """CREATE TABLE IF NOT EXISTS llm_outputs (
            id {serial} PRIMARY KEY,
            post_id TEXT REFERENCES posts(id),
            model_family TEXT,
            model_name TEXT,
            summary TEXT,
            unique_advice_json TEXT,
            divergences_json TEXT,
            clinically_relevant_notes_json TEXT,
            data_quality TEXT
        )""",
        "CREATE INDEX IF NOT EXISTS idx_llm_post ON llm_outputs(post_id)",
        """CREATE TABLE IF NOT EXISTS label_verifications (
            id {serial} PRIMARY KEY,
            post_id TEXT REFERENCES posts(id),
            expert_username TEXT,
            label1_correct INTEGER,
            label2_correct INTEGER,
            label3_correct INTEGER,
            suggested_label TEXT,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(post_id, expert_username)
        )""",
        """CREATE TABLE IF NOT EXISTS comment_verifications (
            id {serial} PRIMARY KEY,
            post_id TEXT REFERENCES posts(id),
            comment_index INTEGER,
            expert_username TEXT,
            flag TEXT,
            note TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(post_id, comment_index, expert_username)
        )""",
        """CREATE TABLE IF NOT EXISTS claim_annotations (
            id {serial} PRIMARY KEY,
            post_id TEXT REFERENCES posts(id),
            comment_index INTEGER,
            expert_username TEXT,
            claim_text TEXT NOT NULL,
            credibility TEXT NOT NULL,
            evidence_type TEXT NOT NULL,
            note TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        "CREATE INDEX IF NOT EXISTS idx_claims_post ON claim_annotations(post_id)",
        "CREATE INDEX IF NOT EXISTS idx_claims_comment ON claim_annotations(post_id, comment_index)",
        """CREATE TABLE IF NOT EXISTS comment_spans (
            id {serial} PRIMARY KEY,
            post_id TEXT REFERENCES posts(id),
            comment_index INTEGER,
            annotator_username TEXT,
            span_start INTEGER NOT NULL,
            span_end INTEGER NOT NULL,
            span_text TEXT NOT NULL,
            code TEXT NOT NULL,
            note TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        "CREATE INDEX IF NOT EXISTS idx_comment_spans_post ON comment_spans(post_id)",
        "CREATE INDEX IF NOT EXISTS idx_comment_spans_lookup ON comment_spans(post_id, comment_index, annotator_username)",
        """CREATE TABLE IF NOT EXISTS comment_codes (
            id {serial} PRIMARY KEY,
            post_id TEXT REFERENCES posts(id),
            comment_index INTEGER,
            annotator_username TEXT,
            code TEXT NOT NULL,
            note TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(post_id, comment_index, annotator_username, code)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_comment_codes_post ON comment_codes(post_id)",
        "CREATE INDEX IF NOT EXISTS idx_comment_codes_lookup ON comment_codes(post_id, annotator_username)",
        """CREATE TABLE IF NOT EXISTS expert_claim_reviews (
            id {serial} PRIMARY KEY,
            post_id TEXT REFERENCES posts(id),
            comment_index INTEGER,
            span_start INTEGER NOT NULL,
            span_end INTEGER NOT NULL,
            span_text TEXT NOT NULL,
            expert_username TEXT NOT NULL,
            verdict TEXT NOT NULL,
            note TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(post_id, comment_index, span_start, span_end, expert_username)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_expert_reviews_post ON expert_claim_reviews(post_id)",
        "CREATE INDEX IF NOT EXISTS idx_expert_reviews_lookup ON expert_claim_reviews(post_id, expert_username)",
        """CREATE TABLE IF NOT EXISTS user_settings (
            username TEXT NOT NULL,
            key TEXT NOT NULL,
            value TEXT,
            PRIMARY KEY(username, key)
        )""",
        """CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'annotator',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
    ]

    serial = "SERIAL" if USE_POSTGRES else "INTEGER PRIMARY KEY AUTOINCREMENT"
    # For Postgres, SERIAL already implies PRIMARY KEY-like behavior but we need to adjust
    if USE_POSTGRES:
        serial = "SERIAL"
        for sql in tables:
            sql = sql.replace("{serial} PRIMARY KEY", "SERIAL PRIMARY KEY")
            conn.execute(sql)
    else:
        for sql in tables:
            sql = sql.replace("{serial} PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")
            conn.execute(sql)

    conn.commit()

    # Migrate: add columns to existing posts table if missing
    _add_column_if_missing(conn, "posts", "assigned_batch", "TEXT")
    _add_column_if_missing(conn, "posts", "assigned_annotator", "TEXT")

    conn.close()


def _add_column_if_missing(conn, table, column, col_type):
    """Add a column to a table if it doesn't already exist (safe for migrations)."""
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
        conn.commit()
    except Exception:
        pass  # Column already exists


def get_posts(label_filter=None, verified_filter=None, search=None, page=1, per_page=25, username=None, assigned_to=None):
    conn = get_db()
    conditions = []
    params = []

    if label_filter:
        conditions.append("(p.label1 = ? OR p.label2 = ? OR p.label3 = ?)")
        params.extend([label_filter, label_filter, label_filter])

    if search:
        conditions.append("(p.title LIKE ? OR p.id LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])

    if assigned_to:
        conditions.append("(p.assigned_annotator = ? AND p.assigned_batch IS NOT NULL AND p.assigned_batch != '')")
        params.append(assigned_to)

    where = ""
    if conditions:
        where = "WHERE " + " AND ".join(conditions)

    count_sql = f"SELECT COUNT(*) FROM posts p {where}"
    total = conn.execute(count_sql, params).fetchone()[0]

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


def get_claims(post_id, username):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM claim_annotations WHERE post_id = ? AND expert_username = ? ORDER BY comment_index, id",
        (post_id, username),
    ).fetchall()
    conn.close()
    claims_by_comment = {}
    for row in rows:
        ci = row["comment_index"]
        if ci not in claims_by_comment:
            claims_by_comment[ci] = []
        claims_by_comment[ci].append(row)
    return claims_by_comment


def save_claims(post_id, username, claims_data):
    conn = get_db()
    conn.execute(
        "DELETE FROM claim_annotations WHERE post_id = ? AND expert_username = ?",
        (post_id, username),
    )
    for claim in claims_data:
        if claim.get("claim_text", "").strip():
            conn.execute(
                """INSERT INTO claim_annotations
                    (post_id, comment_index, expert_username, claim_text, credibility, evidence_type, note)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    post_id,
                    claim["comment_index"],
                    username,
                    claim["claim_text"].strip(),
                    claim.get("credibility", "unclear"),
                    claim.get("evidence_type", "anecdotal"),
                    claim.get("note", ""),
                ),
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
    claims_annotated = conn.execute(
        "SELECT COUNT(*) FROM claim_annotations"
    ).fetchone()[0]
    conn.close()
    return {"total": total, "verified": verified, "comments_flagged": comments_flagged, "claims_annotated": claims_annotated}


def get_comment_spans(post_id, username):
    """Return spans grouped by comment_index."""
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM comment_spans WHERE post_id = ? AND annotator_username = ? ORDER BY comment_index, span_start",
        (post_id, username),
    ).fetchall()
    conn.close()
    spans_by_comment = {}
    for row in rows:
        ci = row["comment_index"]
        if ci not in spans_by_comment:
            spans_by_comment[ci] = []
        spans_by_comment[ci].append(dict(row))
    return spans_by_comment


def save_comment_spans(post_id, username, spans_data):
    """Replace all spans for this post/user with the new set."""
    conn = get_db()
    conn.execute(
        "DELETE FROM comment_spans WHERE post_id = ? AND annotator_username = ?",
        (post_id, username),
    )
    for span in spans_data:
        conn.execute(
            """INSERT INTO comment_spans
                (post_id, comment_index, annotator_username, span_start, span_end, span_text, code, note)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                post_id,
                span["comment_index"],
                username,
                span["span_start"],
                span["span_end"],
                span["span_text"],
                span["code"],
                span.get("note", ""),
            ),
        )
    conn.commit()
    conn.close()


def delete_comment_span(span_id, username):
    conn = get_db()
    conn.execute(
        "DELETE FROM comment_spans WHERE id = ? AND annotator_username = ?",
        (span_id, username),
    )
    conn.commit()
    conn.close()


def get_comment_codes(post_id, username):
    """Return codes grouped by comment_index: {ci: [code1, code2, ...]}"""
    conn = get_db()
    rows = conn.execute(
        "SELECT comment_index, code FROM comment_codes WHERE post_id = ? AND annotator_username = ? ORDER BY comment_index",
        (post_id, username),
    ).fetchall()
    conn.close()
    codes_by_comment = {}
    for row in rows:
        ci = row["comment_index"]
        if ci not in codes_by_comment:
            codes_by_comment[ci] = []
        codes_by_comment[ci].append(row["code"])
    return codes_by_comment


def save_comment_codes(post_id, username, codes_data):
    """codes_data: {comment_index: [code1, code2, ...]}"""
    conn = get_db()
    conn.execute(
        "DELETE FROM comment_codes WHERE post_id = ? AND annotator_username = ?",
        (post_id, username),
    )
    for comment_index, codes in codes_data.items():
        for code in codes:
            conn.execute(
                """INSERT INTO comment_codes
                    (post_id, comment_index, annotator_username, code)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(post_id, comment_index, annotator_username, code) DO NOTHING""",
                (post_id, int(comment_index), username, code),
            )
    conn.commit()
    conn.close()


def get_post_code_summaries(post_ids, username):
    """Return {post_id: list of codes} for the given posts and user."""
    if not post_ids:
        return {}
    conn = get_db()
    placeholders = ",".join(["?"] * len(post_ids))
    rows = conn.execute(
        f"SELECT post_id, code FROM comment_codes WHERE post_id IN ({placeholders}) AND annotator_username = ?",
        list(post_ids) + [username],
    ).fetchall()
    conn.close()
    summaries = {}
    for row in rows:
        pid = row["post_id"]
        if pid not in summaries:
            summaries[pid] = []
        if row["code"] not in summaries[pid]:
            summaries[pid].append(row["code"])
    return summaries


def get_annotation_progress(username):
    conn = get_db()
    total_posts = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    annotated_posts = conn.execute(
        "SELECT COUNT(DISTINCT post_id) FROM comment_codes WHERE annotator_username = ?",
        (username,),
    ).fetchone()[0]
    total_coded = conn.execute(
        "SELECT COUNT(*) FROM comment_codes WHERE annotator_username = ?",
        (username,),
    ).fetchone()[0]
    claim_count = conn.execute(
        "SELECT COUNT(*) FROM comment_codes WHERE annotator_username = ? AND code = 'CLAIM'",
        (username,),
    ).fetchone()[0]
    total_spans = conn.execute(
        "SELECT COUNT(*) FROM comment_spans WHERE annotator_username = ?",
        (username,),
    ).fetchone()[0]
    conn.close()
    return {
        "total_posts": total_posts,
        "annotated_posts": annotated_posts,
        "total_coded": total_coded,
        "claim_count": claim_count,
        "total_spans": total_spans,
    }


def get_claim_spans_for_review(post_id):
    """Get all CLAIM spans from all annotators for a given post."""
    conn = get_db()
    rows = conn.execute(
        """SELECT DISTINCT comment_index, span_start, span_end, span_text, annotator_username
           FROM comment_spans
           WHERE post_id = ? AND code = 'CLAIM'
           ORDER BY comment_index, span_start""",
        (post_id,),
    ).fetchall()
    conn.close()
    claims_by_comment = {}
    for row in rows:
        ci = row["comment_index"]
        if ci not in claims_by_comment:
            claims_by_comment[ci] = []
        claims_by_comment[ci].append(dict(row))
    return claims_by_comment


def get_expert_reviews(post_id, expert_username):
    """Get existing expert reviews keyed by (comment_index, span_start, span_end)."""
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM expert_claim_reviews WHERE post_id = ? AND expert_username = ?",
        (post_id, expert_username),
    ).fetchall()
    conn.close()
    reviews = {}
    for row in rows:
        key = (row["comment_index"], row["span_start"], row["span_end"])
        reviews[key] = dict(row)
    return reviews


def save_expert_reviews(post_id, expert_username, reviews_data):
    """reviews_data: list of {comment_index, span_start, span_end, span_text, verdict, note}"""
    conn = get_db()
    for r in reviews_data:
        conn.execute(
            """INSERT INTO expert_claim_reviews
                (post_id, comment_index, span_start, span_end, span_text, expert_username, verdict, note)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(post_id, comment_index, span_start, span_end, expert_username) DO UPDATE SET
                verdict=excluded.verdict, note=excluded.note, created_at=CURRENT_TIMESTAMP
            """,
            (post_id, r["comment_index"], r["span_start"], r["span_end"],
             r["span_text"], expert_username, r["verdict"], r.get("note", "")),
        )
    conn.commit()
    conn.close()


def get_posts_with_claims(page=1, per_page=25, search=None):
    """Get posts that have at least one CLAIM span from annotators."""
    conn = get_db()
    conditions = []
    params = []
    if search:
        conditions.append("(p.title LIKE ? OR p.id LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])

    where = ""
    if conditions:
        where = "AND " + " AND ".join(conditions)

    count_sql = f"""SELECT COUNT(DISTINCT p.id) FROM posts p
        INNER JOIN comment_spans cs ON cs.post_id = p.id AND cs.code = 'CLAIM'
        WHERE 1=1 {where}"""
    total = conn.execute(count_sql, params).fetchone()[0]

    sql = f"""SELECT p.*,
        (SELECT COUNT(*) FROM comment_spans cs WHERE cs.post_id = p.id AND cs.code = 'CLAIM') as claim_count,
        (SELECT COUNT(DISTINCT ecr.comment_index || '-' || ecr.span_start)
         FROM expert_claim_reviews ecr WHERE ecr.post_id = p.id) as reviewed_count
        FROM posts p
        INNER JOIN comment_spans cs2 ON cs2.post_id = p.id AND cs2.code = 'CLAIM'
        WHERE 1=1 {where}
        GROUP BY p.id
        ORDER BY claim_count DESC
        LIMIT ? OFFSET ?"""
    params.extend([per_page, (page - 1) * per_page])
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return rows, total


def get_expert_review_progress(expert_username):
    conn = get_db()
    total_claims = conn.execute(
        "SELECT COUNT(*) FROM comment_spans WHERE code = 'CLAIM'"
    ).fetchone()[0]
    reviewed = conn.execute(
        "SELECT COUNT(*) FROM expert_claim_reviews WHERE expert_username = ?",
        (expert_username,),
    ).fetchone()[0]
    posts_with_claims = conn.execute(
        "SELECT COUNT(DISTINCT post_id) FROM comment_spans WHERE code = 'CLAIM'"
    ).fetchone()[0]
    posts_reviewed = conn.execute(
        "SELECT COUNT(DISTINCT post_id) FROM expert_claim_reviews WHERE expert_username = ?",
        (expert_username,),
    ).fetchone()[0]
    conn.close()
    return {
        "total_claims": total_claims,
        "reviewed": reviewed,
        "posts_with_claims": posts_with_claims,
        "posts_reviewed": posts_reviewed,
    }


def posts_loaded():
    conn = get_db()
    try:
        count = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
        conn.close()
        return count > 0
    except Exception:
        conn.close()
        return False


def get_user_setting(username, key):
    conn = get_db()
    row = conn.execute(
        "SELECT value FROM user_settings WHERE username = ? AND key = ?",
        (username, key),
    ).fetchone()
    conn.close()
    return row["value"] if row else None


def set_user_setting(username, key, value):
    conn = get_db()
    conn.execute(
        """INSERT INTO user_settings (username, key, value) VALUES (?, ?, ?)
           ON CONFLICT(username, key) DO UPDATE SET value = excluded.value""",
        (username, key, value),
    )
    conn.commit()
    conn.close()


def register_user(username, password, role="annotator"):
    """Create a new user account. Returns (success, message)."""
    conn = get_db()
    existing = conn.execute("SELECT username FROM users WHERE username = ?", (username,)).fetchone()
    if existing:
        conn.close()
        return False, "Username already taken"
    conn.execute(
        "INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)",
        (username, generate_password_hash(password), role),
    )
    conn.commit()
    conn.close()
    return True, "Account created"


def authenticate_user(username, password):
    """Check credentials. Returns the user dict or None."""
    conn = get_db()
    row = conn.execute("SELECT username, password_hash, role FROM users WHERE username = ?", (username,)).fetchone()
    conn.close()
    if row and check_password_hash(row["password_hash"], password):
        return {"username": row["username"], "role": row["role"]}
    return None


def seed_users(users_dict):
    """Seed the users table from the legacy USERS config dict (idempotent)."""
    conn = get_db()
    for username, info in users_dict.items():
        existing = conn.execute("SELECT username FROM users WHERE username = ?", (username,)).fetchone()
        if not existing:
            conn.execute(
                "INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)",
                (username, generate_password_hash(info["password"]), info["role"]),
            )
    conn.commit()
    conn.close()
