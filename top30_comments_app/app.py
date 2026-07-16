import os
from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from models import db, User, ItemReview, TextAnnotation
from load_data import load_all

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "top30-comments-dev-key-2024")
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///top30.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db.init_app(app)

POST_IDS, POSTS, COMMENTS, MODELS, CLASS_SUMMARIES = [], {}, {}, [], {}


def init_data():
    global POST_IDS, POSTS, COMMENTS, MODELS, CLASS_SUMMARIES
    POST_IDS, POSTS, COMMENTS, MODELS, CLASS_SUMMARIES = load_all()


# ── Auth ──────────────────────────────────────────────

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        if not username or not password:
            return render_template("login.html", error="Username and password required")
        user = User.query.filter_by(username=username).first()
        if not user or not user.check_password(password):
            return render_template("login.html", error="Invalid username or password")
        session["user_id"] = user.id
        session["username"] = user.username
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
        if User.query.filter_by(username=username).first():
            return render_template("register.html", error="Username already taken")
        user = User(username=username)
        user.set_password(password)
        db.session.add(user)
        db.session.commit()
        session["user_id"] = user.id
        session["username"] = user.username
        return redirect(url_for("dashboard"))
    return render_template("register.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


def get_user():
    uid = session.get("user_id")
    if not uid:
        return None
    return User.query.get(uid)


# ── Dashboard ─────────────────────────────────────────

@app.route("/")
def dashboard():
    user = get_user()
    if not user:
        return redirect(url_for("login"))

    search = request.args.get("search", "").strip()
    class_filter = request.args.get("class", "").strip()
    model_filter = request.args.get("model", MODELS[0] if MODELS else "openai")

    posts_list = []
    for pid in POST_IDS:
        key = (pid, model_filter)
        post = POSTS.get(key)
        if not post:
            continue
        if class_filter and post["class_label"] != class_filter:
            continue
        if search and search.lower() not in (post["title"] or "").lower() and search.lower() not in pid.lower():
            continue

        total_items = len(post["advice"]) + len(post["divergences"]) + len(post["clinical_notes"])

        # Count reviewed items
        reviewed = ItemReview.query.filter_by(
            user_id=user.id, post_id=pid, model_name=model_filter
        ).filter(ItemReview.verdict.isnot(None)).count()

        # Count text annotations
        annot_count = TextAnnotation.query.filter_by(
            user_id=user.id, post_id=pid, model_name=model_filter
        ).count()

        posts_list.append({
            "post_id": pid,
            "title": post["title"],
            "class_label": post["class_label"],
            "total_items": total_items,
            "reviewed": reviewed,
            "annotations": annot_count,
            "link": post["link"],
        })

    all_classes = sorted(set(p["class_label"] for (_, m), p in POSTS.items() if m == model_filter))

    # Progress
    total_items = sum(p["total_items"] for p in posts_list)
    total_reviewed = sum(p["reviewed"] for p in posts_list)

    return render_template(
        "dashboard.html",
        posts=posts_list,
        models=MODELS,
        current_model=model_filter,
        search=search,
        all_classes=all_classes,
        current_class=class_filter,
        username=session.get("username"),
        total_posts=len(POST_IDS),
        total_items=total_items,
        total_reviewed=total_reviewed,
    )


# ── Post Detail (with annotation) ────────────────────

@app.route("/post/<post_id>")
def post_detail(post_id):
    user = get_user()
    if not user:
        return redirect(url_for("login"))

    model_filter = request.args.get("model", MODELS[0] if MODELS else "openai")
    key = (post_id, model_filter)
    post = POSTS.get(key)
    if not post:
        return "Post not found", 404

    comment_data = COMMENTS.get(post_id, {})

    # Load existing reviews
    existing_reviews = {}
    for r in ItemReview.query.filter_by(user_id=user.id, post_id=post_id, model_name=model_filter).all():
        existing_reviews[(r.section, r.item_index)] = {"verdict": r.verdict, "note": r.note}

    # Load existing annotations
    existing_annotations = []
    for a in TextAnnotation.query.filter_by(user_id=user.id, post_id=post_id, model_name=model_filter).all():
        existing_annotations.append({
            "id": a.id,
            "section": a.section,
            "item_index": a.item_index,
            "start": a.start_offset,
            "end": a.end_offset,
            "text": a.highlighted_text,
            "annotation": a.annotation_text,
            "verdict": a.verdict,
        })

    # Prev/next navigation
    try:
        idx = POST_IDS.index(post_id)
    except ValueError:
        idx = 0
    prev_id = POST_IDS[idx - 1] if idx > 0 else None
    next_id = POST_IDS[idx + 1] if idx < len(POST_IDS) - 1 else None

    # Other model's output for comparison
    other_model = [m for m in MODELS if m != model_filter]
    other_post = POSTS.get((post_id, other_model[0])) if other_model else None

    return render_template(
        "post_detail.html",
        post=post,
        comment_data=comment_data,
        existing_reviews=existing_reviews,
        existing_annotations=existing_annotations,
        models=MODELS,
        current_model=model_filter,
        prev_id=prev_id,
        next_id=next_id,
        other_post=other_post,
        username=session.get("username"),
    )


# ── API: Save item reviews ───────────────────────────

@app.route("/api/review/<post_id>/save", methods=["POST"])
def save_reviews(post_id):
    user = get_user()
    if not user:
        return jsonify({"error": "Not logged in"}), 401

    data = request.json
    model_name = data.get("model_name", "")
    reviews = data.get("reviews", [])

    for r in reviews:
        existing = ItemReview.query.filter_by(
            user_id=user.id,
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
                user_id=user.id,
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
    user = get_user()
    if not user:
        return jsonify({"error": "Not logged in"}), 401

    data = request.json
    section = data["section"]
    item_index = data.get("item_index", 0)
    start = data["start"]
    end = data["end"]

    # Remove overlapping annotations
    overlapping = TextAnnotation.query.filter_by(
        user_id=user.id,
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
        user_id=user.id,
        post_id=post_id,
        model_name=data["model_name"],
        section=section,
        item_index=item_index,
        start_offset=start,
        end_offset=end,
        highlighted_text=data["text"],
        annotation_text=data.get("annotation", ""),
        verdict=data.get("verdict"),
    )
    db.session.add(annot)
    db.session.commit()
    return jsonify({"ok": True, "id": annot.id, "removed_ids": removed_ids})


@app.route("/api/annotation/<int:annot_id>/delete", methods=["POST"])
def delete_annotation(annot_id):
    user = get_user()
    if not user:
        return jsonify({"error": "Not logged in"}), 401
    annot = TextAnnotation.query.get(annot_id)
    if annot and annot.user_id == user.id:
        db.session.delete(annot)
        db.session.commit()
    return jsonify({"ok": True})


# ── Class Summaries ───────────────────────────────────

@app.route("/classes")
def class_summaries():
    if not get_user():
        return redirect(url_for("login"))
    return render_template(
        "class_summaries.html",
        summaries=CLASS_SUMMARIES,
        username=session.get("username"),
    )


@app.route("/class/<class_label>")
def class_detail(class_label):
    if not get_user():
        return redirect(url_for("login"))
    summary = CLASS_SUMMARIES.get(class_label)
    if not summary:
        return "Class not found", 404
    return render_template(
        "class_detail.html",
        summary=summary,
        username=session.get("username"),
    )


# ── Startup ───────────────────────────────────────────

# Seeded accounts (passwords reset on every startup so credentials stay reliable)
SEED_USERS = {
    "admin": "admin123",
    "dr_smith": "password123",
    "dr_jones": "password456",
    "anusha": "anusha2026",
    "sarah": "sarah2026",
    "nikhil": "nikhil2026",
    "reet": "reet2026",
    "mehar": "mehar2026",
    "ruiqi": "ruiqi2026",
    "annotator5": "annotate500",
    "annotator6": "annotate600",
    "annotator7": "annotate700",
    "annotator8": "annotate800",
    "annotator9": "annotate900",
    "annotator10": "annotate1000",
}


def seed_users():
    """Create or reset passwords for known team accounts."""
    for username, password in SEED_USERS.items():
        user = User.query.filter_by(username=username).first()
        if not user:
            user = User(username=username)
            db.session.add(user)
        user.set_password(password)
    db.session.commit()


with app.app_context():
    db.create_all()
    seed_users()
    init_data()

if __name__ == "__main__":
    app.run(debug=True, port=5002, use_reloader=False)
