import os
from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from models import db, Expert, Assignment, ItemReview, TextAnnotation
from load_data import load_all

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "expert-review-dev-key-2024")
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get(
    "DATABASE_URL", "sqlite:///expert_reviews.db"
).replace("postgres://", "postgresql://", 1)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db.init_app(app)

# Load data once at startup
POST_IDS, POSTS, COMMENTS, MODELS = [], {}, {}, []


def init_data():
    global POST_IDS, POSTS, COMMENTS, MODELS
    POST_IDS, POSTS, COMMENTS, MODELS = load_all()


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
            # First login for legacy account — set their password
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
    model_name = "comment_annotations"
    is_admin = expert.username == "admin"

    # Get assigned post IDs for this expert (admin sees everything)
    if not is_admin:
        assigned_ids = {a.post_id for a in Assignment.query.filter_by(expert_id=expert.id).all()}
        if not assigned_ids:
            return render_template(
                "dashboard.html",
                posts=[],
                search=search,
                username=session.get("username"),
                is_admin=False,
            )

    # Build post list
    posts_list = []
    for pid in POST_IDS:
        if not is_admin and pid not in assigned_ids:
            continue

        key = (pid, model_name)
        post = POSTS.get(key)
        if not post:
            continue

        if search and search.lower() not in (post["title"] or "").lower() and search.lower() not in pid.lower():
            continue

        if is_admin:
            # Admin sees total annotations across all experts
            annot_count = TextAnnotation.query.filter_by(
                post_id=pid, model_name=model_name
            ).count()
            # Count how many distinct experts have annotated this post
            annotator_count = db.session.query(
                db.func.count(db.func.distinct(TextAnnotation.expert_id))
            ).filter_by(post_id=pid, model_name=model_name).scalar() or 0
        else:
            annot_count = TextAnnotation.query.filter_by(
                expert_id=expert.id, post_id=pid, model_name=model_name
            ).count()
            annotator_count = 0

        posts_list.append({
            "post_id": pid,
            "title": post["title"],
            "class_label": post["class_label"],
            "num_comments": len(post["advice"]),
            "annotations": annot_count,
            "annotator_count": annotator_count,
            "link": post["link"],
            "split": post.get("split", ""),
        })

    # Sort: dev first, then by most comments descending
    split_order = {"dev": 0, "test": 1}
    posts_list.sort(key=lambda p: (split_order.get(p["split"], 2), -p["num_comments"]))

    return render_template(
        "dashboard.html",
        posts=posts_list,
        search=search,
        username=session.get("username"),
        is_admin=is_admin,
    )


# ── Review Page ───────────────────────────────────────

@app.route("/review/<post_id>")
def review(post_id):
    expert = get_expert()
    if not expert:
        return redirect(url_for("login"))

    # Check that expert is assigned to this post (admin can access all)
    if expert.username != "admin":
        assigned_ids = {a.post_id for a in Assignment.query.filter_by(expert_id=expert.id).all()}
        if post_id not in assigned_ids:
            return "Not assigned to this post", 403

    model_name = "comment_annotations"
    key = (post_id, model_name)
    post = POSTS.get(key)
    if not post:
        return "Post not found", 404

    comment_data = COMMENTS.get(post_id, {})

    # Load existing reviews
    existing_reviews = {}
    for r in ItemReview.query.filter_by(expert_id=expert.id, post_id=post_id, model_name=model_name).all():
        existing_reviews[(r.section, r.item_index)] = {"verdict": r.verdict, "note": r.note}

    # Load existing annotations
    existing_annotations = []
    for a in TextAnnotation.query.filter_by(expert_id=expert.id, post_id=post_id, model_name=model_name).all():
        existing_annotations.append({
            "id": a.id,
            "section": a.section,
            "item_index": a.item_index,
            "start": a.start_offset,
            "end": a.end_offset,
            "text": a.highlighted_text,
            "annotation": a.annotation_text,
            "verdict": a.verdict,
            "harm_reasoning": a.harm_reasoning or "",
            "is_gt_span": a.is_gt_span or False,
        })

    # Prev/next navigation (only within assigned posts)
    if assigned_ids:
        nav_ids = [pid for pid in POST_IDS if pid in assigned_ids]
    else:
        nav_ids = POST_IDS
    try:
        idx = nav_ids.index(post_id)
    except ValueError:
        idx = 0
    prev_id = nav_ids[idx - 1] if idx > 0 else None
    next_id = nav_ids[idx + 1] if idx < len(nav_ids) - 1 else None

    return render_template(
        "review.html",
        post=post,
        comment_data=comment_data,
        existing_reviews=existing_reviews,
        existing_annotations=existing_annotations,
        current_model=model_name,
        prev_id=prev_id,
        next_id=next_id,
        username=session.get("username"),
    )


# ── API: Save item reviews ───────────────────────────

@app.route("/api/review/<post_id>/save", methods=["POST"])
def save_reviews(post_id):
    expert = get_expert()
    if not expert:
        return jsonify({"error": "Not logged in"}), 401

    data = request.json
    model_name = data.get("model_name", "")
    reviews = data.get("reviews", [])

    for r in reviews:
        existing = ItemReview.query.filter_by(
            expert_id=expert.id,
            post_id=post_id,
            model_name=model_name,
            section=r["section"],
            item_index=r["item_index"],
        ).first()

        if existing:
            existing.verdict = r.get("verdict")
            existing.note = r.get("note", "")
        else:
            new_review = ItemReview(
                expert_id=expert.id,
                post_id=post_id,
                model_name=model_name,
                section=r["section"],
                item_index=r["item_index"],
                verdict=r.get("verdict"),
                note=r.get("note", ""),
            )
            db.session.add(new_review)

    db.session.commit()
    return jsonify({"ok": True})


# ── API: Save text annotation ────────────────────────

@app.route("/api/annotation/<post_id>/save", methods=["POST"])
def save_annotation(post_id):
    expert = get_expert()
    if not expert:
        return jsonify({"error": "Not logged in"}), 401

    data = request.json
    section = data["section"]
    item_index = data.get("item_index", 0)
    start = data["start"]
    end = data["end"]

    # Remove any overlapping annotations on the same section/item
    overlapping = TextAnnotation.query.filter_by(
        expert_id=expert.id,
        post_id=post_id,
        model_name=data["model_name"],
        section=section,
        item_index=item_index,
    ).filter(
        TextAnnotation.start_offset < end,
        TextAnnotation.end_offset > start,
    ).all()
    removed_ids = [a.id for a in overlapping]
    for a in overlapping:
        db.session.delete(a)

    annot = TextAnnotation(
        expert_id=expert.id,
        post_id=post_id,
        model_name=data["model_name"],
        section=section,
        item_index=item_index,
        start_offset=start,
        end_offset=end,
        highlighted_text=data["text"],
        annotation_text=data.get("annotation", ""),
        verdict=data.get("verdict"),
        harm_reasoning=data.get("harm_reasoning", ""),
        is_gt_span=data.get("is_gt_span", False),
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


# ── Admin: View all annotations on a post ─────────────

@app.route("/admin/review/<post_id>")
def admin_review(post_id):
    expert = get_expert()
    if not expert or expert.username != "admin":
        return redirect(url_for("login"))

    model_name = "comment_annotations"
    key = (post_id, model_name)
    post = POSTS.get(key)
    if not post:
        return "Post not found", 404

    comment_data = COMMENTS.get(post_id, {})

    # Load ALL experts' annotations for this post
    all_annotations = []
    annotations_by_expert = {}
    for a in TextAnnotation.query.filter_by(post_id=post_id, model_name=model_name).all():
        expert_obj = Expert.query.get(a.expert_id)
        annot = {
            "id": a.id,
            "expert_id": a.expert_id,
            "expert_name": expert_obj.username if expert_obj else "unknown",
            "section": a.section,
            "item_index": a.item_index,
            "start": a.start_offset,
            "end": a.end_offset,
            "text": a.highlighted_text,
            "annotation": a.annotation_text,
            "verdict": a.verdict,
            "harm_reasoning": a.harm_reasoning or "",
        }
        all_annotations.append(annot)
        if annot["expert_name"] not in annotations_by_expert:
            annotations_by_expert[annot["expert_name"]] = []
        annotations_by_expert[annot["expert_name"]].append(annot)

    # Prev/next nav across all posts
    try:
        idx = POST_IDS.index(post_id)
    except ValueError:
        idx = 0
    prev_id = POST_IDS[idx - 1] if idx > 0 else None
    next_id = POST_IDS[idx + 1] if idx < len(POST_IDS) - 1 else None

    return render_template(
        "admin_review.html",
        post=post,
        comment_data=comment_data,
        all_annotations=all_annotations,
        annotations_by_expert=annotations_by_expert,
        expert_names=sorted(annotations_by_expert.keys()),
        current_model=model_name,
        prev_id=prev_id,
        next_id=next_id,
        username=session.get("username"),
    )


# ── Admin: Manage experts and assignments ─────────────

@app.route("/admin", methods=["GET"])
def admin():
    expert = get_expert()
    if not expert or expert.username != "admin":
        return redirect(url_for("login"))

    experts = Expert.query.all()
    assignments = {}
    for e in experts:
        assignments[e.id] = [a.post_id for a in Assignment.query.filter_by(expert_id=e.id).all()]

    return render_template(
        "admin.html",
        experts=experts,
        assignments=assignments,
        all_post_ids=POST_IDS,
        username=session.get("username"),
    )


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
    db.session.add(new_expert)
    db.session.commit()
    return jsonify({"ok": True, "id": new_expert.id})


@app.route("/admin/assign", methods=["POST"])
def admin_assign():
    expert = get_expert()
    if not expert or expert.username != "admin":
        return jsonify({"error": "Unauthorized"}), 403

    data = request.json
    expert_id = data.get("expert_id")
    post_ids = data.get("post_ids", [])

    # Remove existing assignments for this expert
    Assignment.query.filter_by(expert_id=expert_id).delete()

    # Add new assignments
    for pid in post_ids:
        db.session.add(Assignment(expert_id=expert_id, post_id=pid))

    db.session.commit()
    return jsonify({"ok": True})


# ── Startup ───────────────────────────────────────────

# Seeded accounts (passwords reset on every startup so credentials stay reliable)
SEED_USERS = {
    "admin": "admin123",
}


def seed_users():
    """Create or reset admin account and remove old seeded experts."""
    # Remove all non-admin experts and their assignments
    old_experts = Expert.query.filter(Expert.username != "admin").all()
    for e in old_experts:
        Assignment.query.filter_by(expert_id=e.id).delete()
        TextAnnotation.query.filter_by(expert_id=e.id).delete()
        ItemReview.query.filter_by(expert_id=e.id).delete()
        db.session.delete(e)

    # Ensure admin exists
    for username, password in SEED_USERS.items():
        expert = Expert.query.filter_by(username=username).first()
        if not expert:
            expert = Expert(username=username)
            db.session.add(expert)
        expert.set_password(password)
    db.session.commit()


def migrate_schema():
    """Add missing columns to existing tables (SQLAlchemy create_all won't alter tables)."""
    from sqlalchemy import inspect, text
    inspector = inspect(db.engine)

    # Add password_hash to expert table if missing
    if "expert" in inspector.get_table_names():
        cols = [c["name"] for c in inspector.get_columns("expert")]
        if "password_hash" not in cols:
            db.session.execute(text("ALTER TABLE expert ADD COLUMN password_hash VARCHAR(256)"))
            db.session.commit()

    # Add missing columns to text_annotation table
    if "text_annotation" in inspector.get_table_names():
        cols = [c["name"] for c in inspector.get_columns("text_annotation")]
        if "item_index" not in cols:
            db.session.execute(text("ALTER TABLE text_annotation ADD COLUMN item_index INTEGER DEFAULT 0"))
        if "harm_reasoning" not in cols:
            db.session.execute(text("ALTER TABLE text_annotation ADD COLUMN harm_reasoning TEXT DEFAULT ''"))
        if "is_gt_span" not in cols:
            db.session.execute(text("ALTER TABLE text_annotation ADD COLUMN is_gt_span BOOLEAN DEFAULT 0"))
        db.session.commit()


def assign_all_posts():
    """Auto-assign all posts to all experts (no assignments = see everything)."""
    # If no assignments exist yet, all experts see all posts by default.
    # This function can be used to explicitly assign posts if needed.
    pass


with app.app_context():
    db.create_all()
    migrate_schema()
    seed_users()
    init_data()
    assign_all_posts()

if __name__ == "__main__":
    app.run(debug=True, port=5001, use_reloader=False)
