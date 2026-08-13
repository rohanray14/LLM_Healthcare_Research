import os, io, csv
from flask import Flask, render_template, request, jsonify, session, redirect, url_for, Response
from models import db, Expert, Assignment, TextAnnotation
from load_data import load_all

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "methadone-review-dev-key-2024")
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get(
    "DATABASE_URL", "sqlite:///methadone_reviews.db"
).replace("postgres://", "postgresql://", 1)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db.init_app(app)

POST_IDS, POSTS, COMMENTS = [], {}, {}


def init_data():
    global POST_IDS, POSTS, COMMENTS
    POST_IDS, POSTS, COMMENTS = load_all()


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
                                   username=session.get("username"), is_admin=False)

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
            annot_count = TextAnnotation.query.filter_by(post_id=pid).count()
            annotator_count = db.session.query(
                db.func.count(db.func.distinct(TextAnnotation.expert_id))
            ).filter_by(post_id=pid).scalar() or 0
        else:
            annot_count = TextAnnotation.query.filter_by(expert_id=expert.id, post_id=pid).count()
            annotator_count = 0

        posts_list.append({
            "post_id": pid,
            "title": post["title"],
            "num_comments": len(post["advice"]),
            "annotations": annot_count,
            "annotator_count": annotator_count,
            "link": post["link"],
        })

    posts_list.sort(key=lambda p: -p["num_comments"])

    return render_template("dashboard.html", posts=posts_list, search=search,
                           username=session.get("username"), is_admin=is_admin)


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
        })

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
    )
    db.session.add(annot)
    db.session.commit()
    return jsonify({"ok": True, "id": annot.id, "removed_ids": removed_ids})


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

    return render_template("admin.html", experts=experts, assignments=assignments,
                           taken_by=taken_by, post_meta=post_meta,
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
    TextAnnotation.query.filter_by(expert_id=expert_id).delete()
    Assignment.query.filter_by(expert_id=expert_id).delete()
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
        "highlighted_span", "start_offset", "end_offset",
        "accuracy", "harm_potential",
        "accuracy_reasoning", "harm_reasoning", "optional_comment", "created_at"
    ])

    for a in TextAnnotation.query.order_by(TextAnnotation.post_id, TextAnnotation.item_index, TextAnnotation.start_offset).all():
        expert_obj = Expert.query.get(a.expert_id)
        writer.writerow([
            a.id,
            expert_obj.username if expert_obj else "unknown",
            a.post_id,
            a.item_index,
            a.highlighted_text,
            a.start_offset,
            a.end_offset,
            a.verdict or "",
            a.harm_verdict or "",
            a.factual_reasoning or "",
            a.harm_reasoning or "",
            a.annotation_text or "",
            a.created_at.isoformat() if a.created_at else "",
        ])

    output.seek(0)
    return Response(output.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=methadone_annotations.csv"})


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

    return render_template("admin_review.html", post=post, comment_data=comment_data,
                           all_annotations=all_annotations,
                           annotations_by_expert=annotations_by_expert,
                           expert_names=sorted(annotations_by_expert.keys()),
                           prev_id=prev_id, next_id=next_id,
                           username=session.get("username"))


# ── Startup ───────────────────────────────────────────

SEED_USERS = {"admin": "admin123"}


def seed_users():
    for username, password in SEED_USERS.items():
        expert = Expert.query.filter_by(username=username).first()
        if not expert:
            expert = Expert(username=username)
            db.session.add(expert)
        expert.set_password(password)
        expert.password_plain = password
    db.session.commit()


with app.app_context():
    db.create_all()
    seed_users()
    init_data()

if __name__ == "__main__":
    app.run(debug=True, port=5002, use_reloader=False)
