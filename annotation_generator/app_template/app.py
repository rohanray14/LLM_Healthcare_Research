import os, io, csv, json, re
from pathlib import Path
from functools import wraps
from flask import Flask, render_template, request, jsonify, session, redirect, url_for, Response, g, abort
from models import db, User, Project, ProjectMember, Assignment, TextAnnotation
from load_data import load_project, clear_cache, MODEL_NAME

BASE = Path(__file__).resolve().parent
DATA_DIR = BASE / "data"

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "annotation-tool-dev-key")
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get(
    "DATABASE_URL", "sqlite:///annotations.db"
).replace("postgres://", "postgresql://", 1)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["MAX_CONTENT_LENGTH"] = 100 * 1024 * 1024

db.init_app(app)


# ── Helpers ────────────────────────────────────────────

def get_user():
    uid = session.get("user_id")
    if not uid:
        return None
    return User.query.get(uid)


def login_required(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if not get_user():
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapped


def get_project_and_role(slug):
    """Returns (project, role) or aborts. role is 'admin' or 'annotator'."""
    project = Project.query.filter_by(slug=slug).first()
    if not project:
        abort(404)
    user = get_user()
    if not user:
        abort(401)
    if project.owner_id == user.id:
        return project, "admin"
    member = ProjectMember.query.filter_by(project_id=project.id, user_id=user.id).first()
    if not member:
        abort(403)
    return project, member.role


def get_project_config(project):
    return json.loads(project.config_json) if project.config_json else {}


def get_project_data(project):
    config = get_project_config(project)
    return load_project(project.slug, config)


def make_slug(name):
    slug = re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-')
    if not slug:
        slug = "project"
    base = slug
    counter = 1
    while Project.query.filter_by(slug=slug).first():
        slug = f"{base}-{counter}"
        counter += 1
    return slug


@app.context_processor
def inject_globals():
    return {"current_user": get_user()}


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
        next_url = request.args.get("next") or url_for("home")
        return redirect(next_url)
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
        return redirect(url_for("home"))
    return render_template("register.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ── Home / Projects ──────────────────────────────────

@app.route("/")
def home():
    user = get_user()
    if not user:
        return render_template("landing.html")
    owned = Project.query.filter_by(owner_id=user.id).order_by(Project.created_at.desc()).all()
    memberships = ProjectMember.query.filter_by(user_id=user.id).all()
    member_projects = [Project.query.get(m.project_id) for m in memberships if m.project_id not in {p.id for p in owned}]
    return render_template("home.html", owned_projects=owned, member_projects=member_projects)


# ── Create Project ───────────────────────────────────

@app.route("/new", methods=["GET"])
@login_required
def new_project():
    return render_template("new_project.html")


@app.route("/new/preview", methods=["POST"])
@login_required
def new_project_preview():
    f = request.files.get("datafile")
    if not f or not f.filename:
        return jsonify({"error": "No file uploaded"}), 400

    ext = f.filename.rsplit(".", 1)[-1].lower()
    if ext == "csv":
        content = f.read().decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(content))
        headers = reader.fieldnames or []
        sample = []
        for i, row in enumerate(reader):
            if i >= 3:
                break
            sample.append(row)
    elif ext in ("xlsx", "xls"):
        try:
            import openpyxl
        except ImportError:
            return jsonify({"error": "Excel support requires openpyxl (pip install openpyxl)"}), 400
        wb = openpyxl.load_workbook(io.BytesIO(f.read()), read_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        wb.close()
        if not rows:
            return jsonify({"error": "Empty file"}), 400
        headers = [str(h or "").strip() for h in rows[0]]
        sample = [{h: str(v or "") for h, v in zip(headers, row)} for row in rows[1:4]]
    else:
        return jsonify({"error": f"Unsupported file type: .{ext}"}), 400

    auto = {}
    for h in headers:
        hl = h.lower().replace(" ", "_")
        if "post_id" in hl or hl == "id":
            auto.setdefault("post_id", h)
        elif "post_title" in hl or hl == "title":
            auto.setdefault("post_title", h)
        elif "post_body" in hl or hl in ("body", "selftext"):
            auto.setdefault("post_body", h)
        elif "comment_id" in hl:
            auto.setdefault("comment_id", h)
        elif "comment_body" in hl or hl in ("comment", "comment_text"):
            auto.setdefault("comment_body", h)
        elif any(k in hl for k in ("theme", "topic", "label", "tag")):
            auto.setdefault("themes", h)

    return jsonify({"headers": headers, "sample": sample, "auto_map": auto})


@app.route("/new/create", methods=["POST"])
@login_required
def new_project_create():
    user = get_user()
    f = request.files.get("datafile")
    if not f or not f.filename:
        return jsonify({"error": "No file uploaded"}), 400

    col_post_id = request.form.get("col_post_id", "").strip()
    col_comment_body = request.form.get("col_comment_body", "").strip()
    if not col_post_id or not col_comment_body:
        return jsonify({"error": "Post ID and Comment Body are required"}), 400

    project_name = request.form.get("app_name", "My Project").strip()
    if not project_name:
        return jsonify({"error": "Project name is required"}), 400

    # Convert Excel to CSV if needed
    ext = f.filename.rsplit(".", 1)[-1].lower()
    if ext == "csv":
        csv_content = f.read().decode("utf-8", errors="replace")
    elif ext in ("xlsx", "xls"):
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(f.read()), read_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        wb.close()
        headers = [str(h or "").strip() for h in rows[0]]
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=headers)
        writer.writeheader()
        for row in rows[1:]:
            writer.writerow({h: str(v or "") for h, v in zip(headers, row)})
        csv_content = output.getvalue()
    else:
        return jsonify({"error": f"Unsupported: .{ext}"}), 400

    slug = make_slug(project_name)

    config = {
        "app_name": project_name,
        "link_template": request.form.get("link_template", "").strip(),
        "instructions": request.form.get("instructions", "").strip(),
        "data_file": "data.csv",
        "columns": {
            "post_id": col_post_id,
            "post_title": request.form.get("col_post_title", "").strip(),
            "post_body": request.form.get("col_post_body", "").strip(),
            "comment_id": request.form.get("col_comment_id", "").strip(),
            "comment_body": col_comment_body,
            "themes": request.form.get("col_themes", "").strip(),
        },
    }

    # Save CSV
    project_dir = DATA_DIR / slug
    project_dir.mkdir(parents=True, exist_ok=True)
    (project_dir / "data.csv").write_text(csv_content, encoding="utf-8")

    # Create project in DB
    project = Project(slug=slug, name=project_name, config_json=json.dumps(config), owner_id=user.id)
    db.session.add(project)
    db.session.commit()

    # Add owner as admin member
    db.session.add(ProjectMember(project_id=project.id, user_id=user.id, role="admin"))
    db.session.commit()

    return jsonify({"ok": True, "slug": slug})


# ── Project Dashboard ────────────────────────────────

@app.route("/p/<slug>/")
@login_required
def project_dashboard(slug):
    project, role = get_project_and_role(slug)
    config = get_project_config(project)
    post_ids, posts, comments = get_project_data(project)
    user = get_user()
    is_admin = (role == "admin")

    search = request.args.get("search", "").strip()
    annotator_filter = request.args.get("annotator", "").strip()
    model_name = MODEL_NAME

    if not is_admin:
        assigned_ids = {a.post_id for a in Assignment.query.filter_by(project_id=project.id, user_id=user.id).all()}
        if not assigned_ids:
            return render_template("dashboard.html", posts=[], search=search, project=project,
                                   config=config, is_admin=False, annotator_filter="",
                                   all_annotator_names=[], total_comments=0, total_annotated=0)

    # Batch DB queries
    if is_admin:
        annot_counts = dict(
            db.session.query(TextAnnotation.post_id, db.func.count(db.func.distinct(TextAnnotation.item_index)))
            .filter_by(project_id=project.id, model_name=model_name).group_by(TextAnnotation.post_id).all()
        )
        assigned_rows = db.session.query(Assignment.post_id, User.username) \
            .join(User, User.id == Assignment.user_id) \
            .filter(Assignment.project_id == project.id).all()
        assigned_map = {}
        for pid_r, uname in assigned_rows:
            assigned_map.setdefault(pid_r, set()).add(uname)
        coded_rows = db.session.query(TextAnnotation.post_id, User.username) \
            .join(User, User.id == TextAnnotation.user_id) \
            .filter(TextAnnotation.project_id == project.id, TextAnnotation.model_name == model_name).distinct().all()
        coded_map = {}
        for pid_r, uname in coded_rows:
            coded_map.setdefault(pid_r, set()).add(uname)
    else:
        annot_counts = dict(
            db.session.query(TextAnnotation.post_id, db.func.count(db.func.distinct(TextAnnotation.item_index)))
            .filter_by(user_id=user.id, project_id=project.id, model_name=model_name).group_by(TextAnnotation.post_id).all()
        )

    posts_list = []
    for pid in post_ids:
        if not is_admin and pid not in assigned_ids:
            continue
        key = (pid, model_name)
        post = posts.get(key)
        if not post:
            continue
        if search and search.lower() not in (post["title"] or "").lower() and search.lower() not in pid.lower():
            continue
        annotated_comments = annot_counts.get(pid, 0)
        assigned_names = sorted(assigned_map.get(pid, set()) | coded_map.get(pid, set())) if is_admin else []
        posts_list.append({
            "post_id": pid, "title": post["title"], "class_label": post["class_label"],
            "num_comments": len(post["advice"]), "annotated_comments": annotated_comments,
            "assigned_names": assigned_names, "link": post["link"],
        })

    all_annotator_names = sorted({name for p in posts_list for name in p.get("assigned_names", [])}) if is_admin else []
    if is_admin and annotator_filter:
        posts_list = [p for p in posts_list if annotator_filter in p.get("assigned_names", [])]
    posts_list.sort(key=lambda p: -p["num_comments"])

    total_comments = sum(p["num_comments"] for p in posts_list)
    total_annotated = sum(p["annotated_comments"] for p in posts_list)

    return render_template("dashboard.html", posts=posts_list, search=search, project=project,
                           config=config, annotator_filter=annotator_filter,
                           all_annotator_names=all_annotator_names, is_admin=is_admin,
                           total_comments=total_comments, total_annotated=total_annotated)


# ── Review Page ──────────────────────────────────────

@app.route("/p/<slug>/review/<post_id>")
@login_required
def review(slug, post_id):
    project, role = get_project_and_role(slug)
    config = get_project_config(project)
    user = get_user()
    is_admin = (role == "admin")

    if not is_admin:
        assigned_ids = {a.post_id for a in Assignment.query.filter_by(project_id=project.id, user_id=user.id).all()}
        if post_id not in assigned_ids:
            return "Not assigned to this post", 403

    post_ids, posts, comments = get_project_data(project)
    model_name = MODEL_NAME
    post = posts.get((post_id, model_name))
    if not post:
        return "Post not found", 404
    comment_data = comments.get(post_id, {})

    existing_annotations = []
    for a in TextAnnotation.query.filter_by(user_id=user.id, project_id=project.id, post_id=post_id, model_name=model_name).all():
        existing_annotations.append({
            "id": a.id, "section": a.section, "item_index": a.item_index,
            "start": a.start_offset, "end": a.end_offset, "text": a.highlighted_text,
            "annotation": a.annotation_text, "verdict": a.verdict,
            "harm_verdict": a.harm_verdict or "", "factual_reasoning": a.factual_reasoning or "",
            "harm_reasoning": a.harm_reasoning or "", "is_gt_span": a.is_gt_span or False,
        })

    if not is_admin:
        nav_ids = [pid for pid in post_ids if pid in assigned_ids]
    else:
        nav_ids = post_ids
    try:
        idx = nav_ids.index(post_id)
    except ValueError:
        idx = 0
    prev_id = nav_ids[idx - 1] if idx > 0 else None
    next_id = nav_ids[idx + 1] if idx < len(nav_ids) - 1 else None

    return render_template("review.html", post=post, comment_data=comment_data, project=project,
                           config=config, existing_annotations=existing_annotations,
                           current_model=model_name, prev_id=prev_id, next_id=next_id)


# ── API ──────────────────────────────────────────────

@app.route("/p/<slug>/api/annotation/<post_id>/save", methods=["POST"])
@login_required
def save_annotation(slug, post_id):
    project, role = get_project_and_role(slug)
    user = get_user()
    data = request.json
    section = data["section"]
    item_index = data.get("item_index", 0)
    start, end = data["start"], data["end"]

    overlapping = TextAnnotation.query.filter_by(
        user_id=user.id, project_id=project.id, post_id=post_id, model_name=data["model_name"],
        section=section, item_index=item_index,
    ).filter(TextAnnotation.start_offset < end, TextAnnotation.end_offset > start).all()
    removed_ids = [a.id for a in overlapping]
    for a in overlapping:
        db.session.delete(a)

    annot = TextAnnotation(
        user_id=user.id, project_id=project.id, post_id=post_id, model_name=data["model_name"],
        section=section, item_index=item_index, start_offset=start, end_offset=end,
        highlighted_text=data["text"], annotation_text=data.get("annotation", ""),
        verdict=data.get("verdict"), harm_verdict=data.get("harm_verdict"),
        factual_reasoning=data.get("factual_reasoning", ""),
        harm_reasoning=data.get("harm_reasoning", ""),
        is_gt_span=data.get("is_gt_span", False),
    )
    db.session.add(annot)
    db.session.commit()
    return jsonify({"ok": True, "id": annot.id, "removed_ids": removed_ids})


@app.route("/p/<slug>/api/annotation/<int:annot_id>/delete", methods=["POST"])
@login_required
def delete_annotation(slug, annot_id):
    project, role = get_project_and_role(slug)
    user = get_user()
    annot = TextAnnotation.query.get(annot_id)
    if annot and annot.user_id == user.id and annot.project_id == project.id:
        db.session.delete(annot)
        db.session.commit()
    return jsonify({"ok": True})


# ── Admin ────────────────────────────────────────────

@app.route("/p/<slug>/admin")
@login_required
def project_admin(slug):
    project, role = get_project_and_role(slug)
    if role != "admin":
        return redirect(url_for("project_dashboard", slug=slug))
    config = get_project_config(project)
    post_ids, posts_data, comments = get_project_data(project)

    # Get all members (annotators)
    members = ProjectMember.query.filter_by(project_id=project.id).all()
    users_map = {m.user_id: User.query.get(m.user_id) for m in members}
    annotators = [{"user": users_map[m.user_id], "member": m} for m in members if m.role == "annotator"]

    assignments = {a["user"].id: [x.post_id for x in Assignment.query.filter_by(project_id=project.id, user_id=a["user"].id).all()] for a in annotators}

    taken_by = {}
    for a in annotators:
        for pid in assignments.get(a["user"].id, []):
            taken_by.setdefault(pid, []).append(a["user"].username)

    post_meta = {}
    for pid in post_ids:
        post = posts_data.get((pid, MODEL_NAME))
        if post:
            post_meta[pid] = {"themes": post["class_label"], "num_comments": len(post["advice"])}

    annotator_stats = {}
    for a in annotators:
        uid = a["user"].id
        total_comments = sum(len(posts_data.get((pid, MODEL_NAME), {}).get("advice", []))
                            for pid in assignments.get(uid, []))
        annotated_comments = db.session.query(
            TextAnnotation.post_id, TextAnnotation.item_index
        ).filter_by(user_id=uid, project_id=project.id).distinct().count()
        annotator_stats[uid] = {"total_comments": total_comments,
                                "annotations": TextAnnotation.query.filter_by(user_id=uid, project_id=project.id).count(),
                                "annotated_comments": annotated_comments}

    return render_template("admin.html", project=project, config=config, annotators=annotators,
                           assignments=assignments, taken_by=taken_by, post_meta=post_meta,
                           annotator_stats=annotator_stats, all_post_ids=post_ids)


@app.route("/p/<slug>/admin/review/<post_id>")
@login_required
def admin_review(slug, post_id):
    project, role = get_project_and_role(slug)
    if role != "admin":
        return redirect(url_for("project_dashboard", slug=slug))
    config = get_project_config(project)
    post_ids, posts_data, comments_data = get_project_data(project)

    model_name = MODEL_NAME
    post = posts_data.get((post_id, model_name))
    if not post:
        return "Post not found", 404
    comment_data = comments_data.get(post_id, {})

    all_annotations = []
    annotations_by_expert = {}
    for a in TextAnnotation.query.filter_by(project_id=project.id, post_id=post_id, model_name=model_name).all():
        user_obj = User.query.get(a.user_id)
        annot = {
            "id": a.id, "expert_id": a.user_id,
            "expert_name": user_obj.username if user_obj else "unknown",
            "section": a.section, "item_index": a.item_index,
            "start": a.start_offset, "end": a.end_offset, "text": a.highlighted_text,
            "annotation": a.annotation_text, "verdict": a.verdict,
            "harm_verdict": a.harm_verdict or "", "factual_reasoning": a.factual_reasoning or "",
            "harm_reasoning": a.harm_reasoning or "",
        }
        all_annotations.append(annot)
        annotations_by_expert.setdefault(annot["expert_name"], []).append(annot)

    try:
        idx = post_ids.index(post_id)
    except ValueError:
        idx = 0
    prev_id = post_ids[idx - 1] if idx > 0 else None
    next_id = post_ids[idx + 1] if idx < len(post_ids) - 1 else None

    return render_template("admin_review.html", post=post, comment_data=comment_data, project=project,
                           config=config, all_annotations=all_annotations,
                           annotations_by_expert=annotations_by_expert,
                           expert_names=sorted(annotations_by_expert.keys()),
                           current_model=model_name, prev_id=prev_id, next_id=next_id)


@app.route("/p/<slug>/admin/export_csv")
@login_required
def admin_export_csv(slug):
    project, role = get_project_and_role(slug)
    if role != "admin":
        return redirect(url_for("project_dashboard", slug=slug))
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["annotation_id", "annotator", "post_id", "comment_index",
                     "highlighted_span", "start_offset", "end_offset",
                     "factual_accuracy", "harm_potential",
                     "factual_reasoning", "harm_reasoning", "optional_comment", "created_at"])
    for a in TextAnnotation.query.filter_by(project_id=project.id).order_by(
            TextAnnotation.post_id, TextAnnotation.item_index, TextAnnotation.start_offset).all():
        user_obj = User.query.get(a.user_id)
        writer.writerow([a.id, user_obj.username if user_obj else "unknown", a.post_id,
                         a.item_index, a.highlighted_text, a.start_offset, a.end_offset,
                         a.verdict or "", a.harm_verdict or "", a.factual_reasoning or "",
                         a.harm_reasoning or "", a.annotation_text or "",
                         a.created_at.isoformat() if a.created_at else ""])
    output.seek(0)
    return Response(output.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={project.slug}_annotations.csv"})


@app.route("/p/<slug>/admin/add_annotator", methods=["POST"])
@login_required
def admin_add_annotator(slug):
    project, role = get_project_and_role(slug)
    if role != "admin":
        return jsonify({"error": "Unauthorized"}), 403
    data = request.json
    username = data.get("username", "").strip()
    password = data.get("password", "").strip()
    if not username or not password:
        return jsonify({"error": "Username and password required"}), 400

    user = User.query.filter_by(username=username).first()
    if not user:
        user = User(username=username)
        user.set_password(password)
        db.session.add(user)
        db.session.flush()

    if ProjectMember.query.filter_by(project_id=project.id, user_id=user.id).first():
        return jsonify({"error": "User already in project"}), 400

    db.session.add(ProjectMember(project_id=project.id, user_id=user.id, role="annotator"))
    db.session.commit()
    return jsonify({"ok": True, "user_id": user.id, "username": user.username})


@app.route("/p/<slug>/admin/remove_annotator", methods=["POST"])
@login_required
def admin_remove_annotator(slug):
    project, role = get_project_and_role(slug)
    if role != "admin":
        return jsonify({"error": "Unauthorized"}), 403
    user_id = request.json.get("user_id")
    if not user_id:
        return jsonify({"error": "user_id required"}), 400
    if user_id == project.owner_id:
        return jsonify({"error": "Cannot remove project owner"}), 400
    TextAnnotation.query.filter_by(project_id=project.id, user_id=user_id).delete()
    Assignment.query.filter_by(project_id=project.id, user_id=user_id).delete()
    ProjectMember.query.filter_by(project_id=project.id, user_id=user_id).delete()
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/p/<slug>/admin/assign", methods=["POST"])
@login_required
def admin_assign(slug):
    project, role = get_project_and_role(slug)
    if role != "admin":
        return jsonify({"error": "Unauthorized"}), 403
    data = request.json
    user_id = data.get("user_id")
    Assignment.query.filter_by(project_id=project.id, user_id=user_id).delete()
    for pid in data.get("post_ids", []):
        if not Assignment.query.filter_by(project_id=project.id, user_id=user_id, post_id=pid).first():
            db.session.add(Assignment(project_id=project.id, user_id=user_id, post_id=pid))
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/p/<slug>/admin/db")
@login_required
def admin_db(slug):
    project, role = get_project_and_role(slug)
    if role != "admin":
        return redirect(url_for("project_dashboard", slug=slug))
    config = get_project_config(project)
    from sqlalchemy import text, inspect
    inspector = inspect(db.engine)
    table_names = inspector.get_table_names()
    selected = request.args.get("table", "")
    rows, columns, row_count = [], [], 0
    if selected and selected in table_names:
        row_count = db.session.execute(text(f'SELECT count(*) FROM "{selected}"')).scalar()
        limit = int(request.args.get("limit", 100))
        offset = int(request.args.get("offset", 0))
        result = db.session.execute(text(f'SELECT * FROM "{selected}" ORDER BY 1 DESC LIMIT :lim OFFSET :off'),
                                    {"lim": limit, "off": offset})
        columns = list(result.keys())
        rows = [list(r) for r in result.fetchall()]
    table_counts = {t: db.session.execute(text(f'SELECT count(*) FROM "{t}"')).scalar() for t in table_names}
    return render_template("admin_db.html", project=project, config=config, tables=table_names,
                           table_counts=table_counts, selected=selected, columns=columns,
                           rows=rows, row_count=row_count,
                           limit=int(request.args.get("limit", 100)),
                           offset=int(request.args.get("offset", 0)))


# ── Join via invite link ─────────────────────────────

@app.route("/join/<slug>")
def join_project(slug):
    project = Project.query.filter_by(slug=slug).first()
    if not project:
        abort(404)
    user = get_user()
    if not user:
        return redirect(url_for("login", next=url_for("join_project", slug=slug)))
    if project.owner_id == user.id:
        return redirect(url_for("project_dashboard", slug=slug))
    existing = ProjectMember.query.filter_by(project_id=project.id, user_id=user.id).first()
    if not existing:
        db.session.add(ProjectMember(project_id=project.id, user_id=user.id, role="annotator"))
        db.session.commit()
    return redirect(url_for("project_dashboard", slug=slug))


# ── Startup ──────────────────────────────────────────

with app.app_context():
    db.create_all()

if __name__ == "__main__":
    app.run(debug=True, port=5001, use_reloader=False)
