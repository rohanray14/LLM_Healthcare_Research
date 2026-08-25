import os, io, csv
from flask import Flask, render_template, request, jsonify, session, redirect, url_for, Response
from models import db, Expert, Assignment, TextAnnotation, CommentCode, PreSeedLog
from load_data import load_all

# Comment-level codes (CLAIM is auto-added via highlighting, not a toggle)
COMMENT_CODES = {
    "CLAIM": {
        "label": "Confident Claim",
        "description": "Any assertion presented with confidence, e.g. \"Suboxone only works when you take it in the morning\" — a claim. Auto-added when you highlight a span.",
        "color": "#fee2e2", "border_color": "#ef4444",
        "is_span_code": True,
    },
    "EXPER": {
        "label": "Personal Experience",
        "description": "Explicitly framed as what happened to the poster personally.",
        "color": "#dbeafe", "border_color": "#3b82f6",
        "is_span_code": False,
    },
    "HEDGED": {
        "label": "Hedged Claim",
        "description": "Uncertainty explicitly signaled. The claim is softened or qualified.",
        "color": "#fef9c3", "border_color": "#eab308",
        "is_span_code": True,
    },
    "SUPPORT": {
        "label": "Emotional Support",
        "description": "No clinical claim. Validates, encourages, or empathizes.",
        "color": "#dcfce7", "border_color": "#22c55e",
        "is_span_code": False,
    },
    "REF": {
        "label": "Referral",
        "description": "Directs the poster to a provider, clinic, or authoritative resource.",
        "color": "#e0e7ff", "border_color": "#6366f1",
        "is_span_code": False,
    },
    "META-R": {
        "label": "Reaction / Advocacy",
        "description": "Emotional reaction, shared distress, or advocacy. No clinical advice.",
        "color": "#f3e8ff", "border_color": "#a855f7",
        "is_span_code": False,
    },
    "EXCLUDE": {
        "label": "Exclude",
        "description": "Comment content is irrelevant and should be excluded from analysis.",
        "color": "#f1f5f9", "border_color": "#64748b",
        "is_span_code": False,
        "needs_reason": True,
    },
}

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "suboxone-review-dev-key-2024")
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get(
    "DATABASE_URL", "sqlite:///suboxone_reviews.db"
).replace("postgres://", "postgresql://", 1)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db.init_app(app)

POST_IDS, POSTS, COMMENTS, PRE_ANNOTATIONS = [], {}, {}, {}


def init_data():
    global POST_IDS, POSTS, COMMENTS, PRE_ANNOTATIONS
    POST_IDS, POSTS, COMMENTS, PRE_ANNOTATIONS = load_all()


# ── Pre-seed annotations from CSV ────────────────────

def preseed_for_expert(expert_id, post_id):
    """Pre-populate l1_coding and claim spans from CSV for this expert+post.
    Only runs once per expert+post (tracked via PreSeedLog)."""
    already = PreSeedLog.query.filter_by(expert_id=expert_id, post_id=post_id).first()
    if already:
        return

    pre = PRE_ANNOTATIONS.get(post_id, {})
    if not pre:
        db.session.add(PreSeedLog(expert_id=expert_id, post_id=post_id))
        db.session.commit()
        return

    for comment_index, data in pre.items():
        # Pre-seed comment-level codes
        for code in data.get("codes", []):
            if code in COMMENT_CODES:
                existing = CommentCode.query.filter_by(
                    expert_id=expert_id, post_id=post_id,
                    comment_index=comment_index, code=code
                ).first()
                if not existing:
                    db.session.add(CommentCode(
                        expert_id=expert_id, post_id=post_id,
                        comment_index=comment_index, code=code,
                    ))

        # Pre-seed CLAIM spans
        for span in data.get("spans", []):
            existing = TextAnnotation.query.filter_by(
                expert_id=expert_id, post_id=post_id,
                item_index=comment_index,
                start_offset=span["start"], end_offset=span["end"],
            ).first()
            if not existing:
                db.session.add(TextAnnotation(
                    expert_id=expert_id, post_id=post_id,
                    item_index=comment_index,
                    start_offset=span["start"], end_offset=span["end"],
                    highlighted_text=span["text"],
                    span_type=span.get("span_type", "CLAIM"),
                ))

    db.session.add(PreSeedLog(expert_id=expert_id, post_id=post_id))
    db.session.commit()


# ── Auth ──────────────────────────────────────────────

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        if not username or not password:
            return render_template("login.html", error="Username and password required")
        expert = Expert.query.filter_by(username=username).first()
        if not expert:
            return render_template("login.html", error="Invalid username or password")
        if not expert.password_hash:
            expert.set_password(password)
            db.session.commit()
        elif not expert.check_password(password):
            return render_template("login.html", error="Invalid username or password")
        session["expert_id"] = expert.id
        session["username"] = expert.username
        return redirect(url_for("dashboard"))
    return render_template("login.html")


@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        confirm = request.form.get("confirm_password", "").strip()
        if not username or not password:
            return render_template("register.html", error="Username and password are required")
        if len(username) < 3:
            return render_template("register.html", error="Username must be at least 3 characters")
        if len(password) < 6:
            return render_template("register.html", error="Password must be at least 6 characters")
        if password != confirm:
            return render_template("register.html", error="Passwords do not match")
        if Expert.query.filter_by(username=username).first():
            return render_template("register.html", error="Username already taken")
        new_expert = Expert(username=username)
        new_expert.set_password(password)
        db.session.add(new_expert)
        db.session.commit()
        session["expert_id"] = new_expert.id
        session["username"] = new_expert.username
        return redirect(url_for("dashboard"))
    return render_template("register.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


def get_expert():
    eid = session.get("expert_id")
    if not eid:
        return None
    return Expert.query.get(eid)


# ── Dashboard ─────────────────────────────────────────

@app.route("/")
def dashboard():
    expert = get_expert()
    if not expert:
        return redirect(url_for("login"))

    search = request.args.get("search", "").strip()
    is_admin = expert.username == "admin"

    if not is_admin:
        assigned_ids = {a.post_id for a in Assignment.query.filter_by(expert_id=expert.id).all()}
        if not assigned_ids:
            return render_template("dashboard.html", posts=[], search=search,
                                   username=session.get("username"), is_admin=False,
                                   total_comments=0, total_coded=0)

    posts_list = []
    for pid in POST_IDS:
        if not is_admin and pid not in assigned_ids:
            continue
        post = POSTS.get(pid)
        if not post:
            continue
        if search and search.lower() not in (post["title"] or "").lower() and search.lower() not in pid.lower():
            continue

        if is_admin:
            coded_count = db.session.query(
                db.func.count(db.func.distinct(CommentCode.comment_index))
            ).filter_by(post_id=pid).scalar() or 0
            annotator_count = db.session.query(
                db.func.count(db.func.distinct(CommentCode.expert_id))
            ).filter_by(post_id=pid).scalar() or 0
            assigned_set = {r.username for r in db.session.query(Expert.username)
                            .join(Assignment, Expert.id == Assignment.expert_id)
                            .filter(Assignment.post_id == pid).all()}
            coded_set = {r.username for r in db.session.query(Expert.username)
                         .join(CommentCode, Expert.id == CommentCode.expert_id)
                         .filter(CommentCode.post_id == pid).distinct().all()}
            assigned_names = sorted(assigned_set | coded_set)
        else:
            coded_count = db.session.query(
                db.func.count(db.func.distinct(CommentCode.comment_index))
            ).filter_by(expert_id=expert.id, post_id=pid).scalar() or 0
            annotator_count = 0
            assigned_names = []

        posts_list.append({
            "post_id": pid,
            "title": post["title"],
            "labels": post.get("labels", ""),
            "num_comments": len(post["advice"]),
            "coded_comments": coded_count,
            "annotator_count": annotator_count,
            "assigned_names": assigned_names,
            "link": post["link"],
        })

    posts_list.sort(key=lambda p: -p["num_comments"])

    total_comments = sum(p["num_comments"] for p in posts_list)
    total_coded = sum(p["coded_comments"] for p in posts_list)

    return render_template("dashboard.html", posts=posts_list, search=search,
                           username=session.get("username"), is_admin=is_admin,
                           total_comments=total_comments, total_coded=total_coded)


# ── Review Page ───────────────────────────────────────

@app.route("/review/<post_id>")
def review(post_id):
    expert = get_expert()
    if not expert:
        return redirect(url_for("login"))

    if expert.username != "admin":
        assigned_ids = {a.post_id for a in Assignment.query.filter_by(expert_id=expert.id).all()}
        if post_id not in assigned_ids:
            return "Not assigned to this post", 403

    post = POSTS.get(post_id)
    if not post:
        return "Post not found", 404

    # Pre-seed from CSV on first visit
    preseed_for_expert(expert.id, post_id)

    comment_data = COMMENTS.get(post_id, {})

    existing_annotations = []
    for a in TextAnnotation.query.filter_by(expert_id=expert.id, post_id=post_id).all():
        existing_annotations.append({
            "id": a.id,
            "item_index": a.item_index,
            "start": a.start_offset,
            "end": a.end_offset,
            "text": a.highlighted_text,
            "annotation": a.annotation_text,
            "verdict": a.verdict,
            "harm_verdict": a.harm_verdict or "",
            "factual_reasoning": a.factual_reasoning or "",
            "harm_reasoning": a.harm_reasoning or "",
            "span_type": a.span_type or "CLAIM",
        })

    # Load comment-level codes
    codes_by_comment = {}
    exclude_reasons = {}
    for cc in CommentCode.query.filter_by(expert_id=expert.id, post_id=post_id).all():
        if cc.comment_index not in codes_by_comment:
            codes_by_comment[cc.comment_index] = []
        codes_by_comment[cc.comment_index].append(cc.code)
        if cc.code == "EXCLUDE" and cc.reason:
            exclude_reasons[cc.comment_index] = cc.reason

    # Prev/next navigation
    if expert.username != "admin":
        nav_ids = [pid for pid in POST_IDS if pid in assigned_ids]
    else:
        nav_ids = POST_IDS
    try:
        idx = nav_ids.index(post_id)
    except ValueError:
        idx = 0
    prev_id = nav_ids[idx - 1] if idx > 0 else None
    next_id = nav_ids[idx + 1] if idx < len(nav_ids) - 1 else None

    return render_template("review.html", post=post, comment_data=comment_data,
                           existing_annotations=existing_annotations,
                           codes_by_comment=codes_by_comment,
                           exclude_reasons=exclude_reasons,
                           comment_codes=COMMENT_CODES,
                           prev_id=prev_id, next_id=next_id,
                           username=session.get("username"))


# ── API: Save annotation ─────────────────────────────

@app.route("/api/annotation/<post_id>/save", methods=["POST"])
def save_annotation(post_id):
    expert = get_expert()
    if not expert:
        return jsonify({"error": "Not logged in"}), 401

    data = request.json
    item_index = data.get("item_index", 0)
    start = data["start"]
    end = data["end"]

    overlapping = TextAnnotation.query.filter_by(
        expert_id=expert.id, post_id=post_id, item_index=item_index,
    ).filter(
        TextAnnotation.start_offset < end,
        TextAnnotation.end_offset > start,
    ).all()
    removed_ids = [a.id for a in overlapping]
    for a in overlapping:
        db.session.delete(a)

    annot = TextAnnotation(
        expert_id=expert.id, post_id=post_id, item_index=item_index,
        start_offset=start, end_offset=end,
        highlighted_text=data["text"],
        annotation_text=data.get("annotation", ""),
        verdict=data.get("verdict"),
        harm_verdict=data.get("harm_verdict"),
        factual_reasoning=data.get("factual_reasoning", ""),
        harm_reasoning=data.get("harm_reasoning", ""),
        span_type=data.get("span_type", "CLAIM"),
    )
    db.session.add(annot)
    db.session.commit()
    return jsonify({"ok": True, "id": annot.id, "removed_ids": removed_ids, "span_type": annot.span_type})


@app.route("/api/codes/<post_id>/save", methods=["POST"])
def save_codes(post_id):
    expert = get_expert()
    if not expert:
        return jsonify({"error": "Not logged in"}), 401
    data = request.json
    comment_index = data.get("comment_index")
    codes = data.get("codes", [])
    exclude_reason = data.get("exclude_reason", "")

    CommentCode.query.filter_by(
        expert_id=expert.id, post_id=post_id, comment_index=comment_index
    ).delete()

    for code in codes:
        if code in COMMENT_CODES:
            db.session.add(CommentCode(
                expert_id=expert.id, post_id=post_id,
                comment_index=comment_index, code=code,
                reason=exclude_reason if code == "EXCLUDE" else "",
            ))
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/api/annotation/<int:annot_id>/delete", methods=["POST"])
def delete_annotation(annot_id):
    expert = get_expert()
    if not expert:
        return jsonify({"error": "Not logged in"}), 401
    annot = TextAnnotation.query.get(annot_id)
    if annot and annot.expert_id == expert.id:
        db.session.delete(annot)
        db.session.commit()
    return jsonify({"ok": True})


# ── Admin ─────────────────────────────────────────────

@app.route("/admin", methods=["GET"])
def admin():
    expert = get_expert()
    if not expert or expert.username != "admin":
        return redirect(url_for("login"))

    experts = Expert.query.all()
    assignments = {}
    for e in experts:
        assignments[e.id] = [a.post_id for a in Assignment.query.filter_by(expert_id=e.id).all()]

    taken_by = {}
    for e in experts:
        if e.username == "admin":
            continue
        for pid in assignments.get(e.id, []):
            if pid not in taken_by:
                taken_by[pid] = []
            taken_by[pid].append(e.username)

    post_meta = {}
    for pid in POST_IDS:
        post = POSTS.get(pid)
        if post:
            post_meta[pid] = {"num_comments": len(post["advice"])}

    expert_stats = {}
    for e in experts:
        if e.username == "admin":
            continue
        total_comments = 0
        total_words = 0
        for pid in assignments.get(e.id, []):
            post = POSTS.get(pid)
            if post:
                for item in post["advice"]:
                    total_comments += 1
                    total_words += len((item.get("advice") or "").split())
        coded_comments = db.session.query(
            db.func.count(db.func.distinct(CommentCode.comment_index))
        ).filter_by(expert_id=e.id).scalar() or 0
        expert_stats[e.id] = {
            "total_comments": total_comments,
            "total_words": total_words,
            "coded_comments": coded_comments,
        }

    return render_template("admin.html", experts=experts, assignments=assignments,
                           taken_by=taken_by, post_meta=post_meta,
                           expert_stats=expert_stats,
                           all_post_ids=POST_IDS, username=session.get("username"))


@app.route("/admin/add_expert", methods=["POST"])
def admin_add_expert():
    expert = get_expert()
    if not expert or expert.username != "admin":
        return jsonify({"error": "Unauthorized"}), 403
    data = request.json
    username = data.get("username", "").strip()
    password = data.get("password", "").strip()
    if not username or not password:
        return jsonify({"error": "Username and password required"}), 400
    if Expert.query.filter_by(username=username).first():
        return jsonify({"error": "Username already exists"}), 400
    new_expert = Expert(username=username)
    new_expert.set_password(password)
    new_expert.password_plain = password
    db.session.add(new_expert)
    db.session.commit()
    return jsonify({"ok": True, "id": new_expert.id})


@app.route("/admin/delete_expert", methods=["POST"])
def admin_delete_expert():
    expert = get_expert()
    if not expert or expert.username != "admin":
        return jsonify({"error": "Unauthorized"}), 403
    data = request.json
    expert_id = data.get("expert_id")
    target = Expert.query.get(expert_id)
    if not target:
        return jsonify({"error": "Expert not found"}), 404
    if target.username == "admin":
        return jsonify({"error": "Cannot delete admin"}), 400
    CommentCode.query.filter_by(expert_id=expert_id).delete()
    TextAnnotation.query.filter_by(expert_id=expert_id).delete()
    Assignment.query.filter_by(expert_id=expert_id).delete()
    PreSeedLog.query.filter_by(expert_id=expert_id).delete()
    db.session.delete(target)
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/admin/assign", methods=["POST"])
def admin_assign():
    expert = get_expert()
    if not expert or expert.username != "admin":
        return jsonify({"error": "Unauthorized"}), 403
    data = request.json
    expert_id = data.get("expert_id")
    post_ids = data.get("post_ids", [])
    Assignment.query.filter_by(expert_id=expert_id).delete()
    for pid in post_ids:
        existing = Assignment.query.filter_by(expert_id=expert_id, post_id=pid).first()
        if not existing:
            db.session.add(Assignment(expert_id=expert_id, post_id=pid))
    db.session.commit()
    return jsonify({"ok": True})


# ── Admin: DB Viewer ──────────────────────────────────

@app.route("/admin/db")
def admin_db():
    expert = get_expert()
    if not expert or expert.username != "admin":
        return redirect(url_for("login"))

    from sqlalchemy import text, inspect
    inspector = inspect(db.engine)
    table_names = inspector.get_table_names()

    selected = request.args.get("table", "")
    rows = []
    columns = []
    row_count = 0

    if selected and selected in table_names:
        result = db.session.execute(text(f'SELECT count(*) FROM "{selected}"'))
        row_count = result.scalar()
        limit = int(request.args.get("limit", 100))
        offset = int(request.args.get("offset", 0))
        result = db.session.execute(
            text(f'SELECT * FROM "{selected}" ORDER BY 1 DESC LIMIT :lim OFFSET :off'),
            {"lim": limit, "off": offset},
        )
        columns = list(result.keys())
        rows = [list(r) for r in result.fetchall()]

    table_counts = {}
    for t in table_names:
        result = db.session.execute(text(f'SELECT count(*) FROM "{t}"'))
        table_counts[t] = result.scalar()

    return render_template(
        "admin_db.html",
        tables=table_names,
        table_counts=table_counts,
        selected=selected,
        columns=columns,
        rows=rows,
        row_count=row_count,
        limit=int(request.args.get("limit", 100)),
        offset=int(request.args.get("offset", 0)),
    )


# ── Admin: Export CSV ─────────────────────────────────

@app.route("/admin/export_csv")
def admin_export_csv():
    expert = get_expert()
    if not expert or expert.username != "admin":
        return redirect(url_for("login"))

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "annotation_id", "expert", "post_id", "comment_index",
        "span_type", "highlighted_span", "start_offset", "end_offset", "created_at"
    ])

    for a in TextAnnotation.query.order_by(TextAnnotation.post_id, TextAnnotation.item_index, TextAnnotation.start_offset).all():
        expert_obj = Expert.query.get(a.expert_id)
        writer.writerow([
            a.id,
            expert_obj.username if expert_obj else "unknown",
            a.post_id,
            a.item_index,
            a.span_type or "CLAIM",
            a.highlighted_text,
            a.start_offset,
            a.end_offset,
            a.created_at.isoformat() if a.created_at else "",
        ])

    codes_output = io.StringIO()
    codes_writer = csv.writer(codes_output)
    codes_writer.writerow(["expert", "post_id", "comment_index", "code", "exclude_reason"])
    for cc in CommentCode.query.order_by(CommentCode.post_id, CommentCode.comment_index).all():
        expert_obj = Expert.query.get(cc.expert_id)
        codes_writer.writerow([
            expert_obj.username if expert_obj else "unknown",
            cc.post_id, cc.comment_index, cc.code, cc.reason or "",
        ])

    combined = output.getvalue() + "\n\n--- COMMENT CODES ---\n" + codes_output.getvalue()
    return Response(combined, mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=suboxone_annotations.csv"})


# ── Admin: View all annotations on a post ─────────────

@app.route("/admin/review/<post_id>")
def admin_review(post_id):
    expert = get_expert()
    if not expert or expert.username != "admin":
        return redirect(url_for("login"))

    post = POSTS.get(post_id)
    if not post:
        return "Post not found", 404

    comment_data = COMMENTS.get(post_id, {})

    all_annotations = []
    annotations_by_expert = {}
    for a in TextAnnotation.query.filter_by(post_id=post_id).all():
        expert_obj = Expert.query.get(a.expert_id)
        annot = {
            "id": a.id,
            "expert_name": expert_obj.username if expert_obj else "unknown",
            "item_index": a.item_index,
            "start": a.start_offset,
            "end": a.end_offset,
            "text": a.highlighted_text,
            "annotation": a.annotation_text,
            "verdict": a.verdict,
            "harm_verdict": a.harm_verdict or "",
            "factual_reasoning": a.factual_reasoning or "",
            "harm_reasoning": a.harm_reasoning or "",
            "span_type": a.span_type or "CLAIM",
        }
        all_annotations.append(annot)
        if annot["expert_name"] not in annotations_by_expert:
            annotations_by_expert[annot["expert_name"]] = []
        annotations_by_expert[annot["expert_name"]].append(annot)

    try:
        idx = POST_IDS.index(post_id)
    except ValueError:
        idx = 0
    prev_id = POST_IDS[idx - 1] if idx > 0 else None
    next_id = POST_IDS[idx + 1] if idx < len(POST_IDS) - 1 else None

    all_codes = {}
    for cc in CommentCode.query.filter_by(post_id=post_id).all():
        expert_obj = Expert.query.get(cc.expert_id)
        name = expert_obj.username if expert_obj else "unknown"
        key = f"{name}_{cc.comment_index}"
        if key not in all_codes:
            all_codes[key] = {"expert_name": name, "comment_index": cc.comment_index, "codes": [], "exclude_reason": ""}
        all_codes[key]["codes"].append(cc.code)
        if cc.code == "EXCLUDE" and cc.reason:
            all_codes[key]["exclude_reason"] = cc.reason

    return render_template("admin_review.html", post=post, comment_data=comment_data,
                           all_annotations=all_annotations,
                           annotations_by_expert=annotations_by_expert,
                           expert_names=sorted(annotations_by_expert.keys()),
                           all_codes=list(all_codes.values()),
                           comment_codes=COMMENT_CODES,
                           prev_id=prev_id, next_id=next_id,
                           username=session.get("username"))


# ── Startup ───────────────────────────────────────────

SEED_USERS = {"admin": "admin123"}


def migrate_schema():
    """Add missing columns to existing tables."""
    import logging
    from sqlalchemy import inspect, text
    try:
        inspector = inspect(db.engine)
        tables = inspector.get_table_names()
        logging.info(f"[migrate] Existing tables: {tables}")

        if "expert" in tables:
            cols = [c["name"] for c in inspector.get_columns("expert")]
            if "password_hash" not in cols:
                db.session.execute(text("ALTER TABLE expert ADD COLUMN password_hash VARCHAR(256)"))
            if "password_plain" not in cols:
                db.session.execute(text("ALTER TABLE expert ADD COLUMN password_plain VARCHAR(256)"))
            db.session.commit()

        if "comment_code" in tables:
            cols = [c["name"] for c in inspector.get_columns("comment_code")]
            if "reason" not in cols:
                db.session.execute(text("ALTER TABLE comment_code ADD COLUMN reason TEXT DEFAULT ''"))
            db.session.commit()

        if "text_annotation" in tables:
            cols = [c["name"] for c in inspector.get_columns("text_annotation")]
            for col, coltype in [
                ("item_index", "INTEGER DEFAULT 0"),
                ("harm_verdict", "VARCHAR(20)"),
                ("factual_reasoning", "TEXT DEFAULT ''"),
                ("harm_reasoning", "TEXT DEFAULT ''"),
                ("span_type", "VARCHAR(20) DEFAULT 'CLAIM'"),
            ]:
                if col not in cols:
                    db.session.execute(text(f"ALTER TABLE text_annotation ADD COLUMN {col} {coltype}"))
            db.session.commit()

        logging.info("[migrate] Schema migration complete")
    except Exception as e:
        logging.error(f"[migrate] Schema migration error: {e}")
        db.session.rollback()


def seed_users():
    for username, password in SEED_USERS.items():
        expert = Expert.query.filter_by(username=username).first()
        if not expert:
            expert = Expert(username=username)
            db.session.add(expert)
            expert.set_password(password)
            expert.password_plain = password
    db.session.commit()


@app.route("/health")
def health():
    try:
        count = Expert.query.count()
        return jsonify({"status": "ok", "experts": count, "posts": len(POST_IDS),
                        "db_uri": "postgresql" if "postgresql" in app.config["SQLALCHEMY_DATABASE_URI"] else "sqlite"})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


import logging
logging.basicConfig(level=logging.INFO)

with app.app_context():
    db_uri = app.config["SQLALCHEMY_DATABASE_URI"]
    logging.info(f"[startup] DB type: {'postgresql' if 'postgresql' in db_uri else 'sqlite'}")
    try:
        db.create_all()
        logging.info("[startup] Tables created/verified")
        migrate_schema()
        seed_users()
        logging.info("[startup] Seed complete")
        init_data()
        logging.info(f"[startup] Loaded {len(POST_IDS)} posts with pre-annotations for {len(PRE_ANNOTATIONS)} posts")
    except Exception as e:
        logging.error(f"[startup] FATAL: {e}")
        raise

if __name__ == "__main__":
    app.run(debug=True, port=5003, use_reloader=False)
