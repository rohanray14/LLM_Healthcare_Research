import os, io, csv, json, re, logging
from flask import Flask, render_template, request, jsonify, session, redirect, url_for, Response
from models import db, Project, Post, Comment, Expert, Assignment, SpanAnnotation, CommentCode, PreSeedLog
from csv_parser import parse_csv_content

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "annotation-tool-dev-key-2024")
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get(
    "DATABASE_URL", "sqlite:///annotation_tool.db"
).replace("postgres://", "postgresql://", 1)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50MB upload limit

db.init_app(app)

GLOBAL_ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin123")


# ── Helpers ──────────────────────────────────────────

def get_project(slug):
    return Project.query.filter_by(slug=slug).first()


def get_expert():
    eid = session.get("expert_id")
    if not eid:
        return None
    return Expert.query.get(eid)


def require_project_admin(slug):
    project = get_project(slug)
    if not project:
        return None, None, ("Project not found", 404)
    expert = get_expert()
    if not expert or expert.username != "admin" or expert.project_id != project.id:
        return project, None, redirect(url_for("login", slug=slug))
    return project, expert, None


def slugify(name):
    slug = re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-')
    # Ensure uniqueness
    base = slug
    counter = 1
    while Project.query.filter_by(slug=slug).first():
        slug = f"{base}-{counter}"
        counter += 1
    return slug


def get_post_ids_for_project(project):
    """Return ordered list of external post_ids for a project."""
    posts = Post.query.filter_by(project_id=project.id).order_by(Post.order_index).all()
    return [p.post_id for p in posts]


def get_post_by_ext_id(project, post_id):
    return Post.query.filter_by(project_id=project.id, post_id=post_id).first()


def get_post_data(post):
    """Build post dict compatible with templates."""
    comments = Comment.query.filter_by(post_ref_id=post.id).order_by(Comment.comment_index).all()
    return {
        "post_id": post.post_id,
        "title": post.title,
        "body": post.body,
        "labels": post.labels,
        "split": post.split,
        "link": post.link,
        "advice": [{"advice": c.comment_body, "comment_id": c.comment_id} for c in comments],
    }


# ── Pre-seed annotations from CSV ────────────────────

def preseed_for_expert(expert_id, post):
    already = PreSeedLog.query.filter_by(expert_id=expert_id, post_id=post.post_id).first()
    if already:
        return

    pre = post.get_pre_annotations()
    config = post.project.get_config()
    comment_codes_config = config.get("comment_codes", {})

    if not pre:
        db.session.add(PreSeedLog(expert_id=expert_id, post_id=post.post_id))
        db.session.commit()
        return

    for comment_index, data in pre.items():
        for code in data.get("codes", []):
            if code in comment_codes_config:
                existing = CommentCode.query.filter_by(
                    expert_id=expert_id, post_id=post.post_id,
                    comment_index=comment_index, code=code
                ).first()
                if not existing:
                    db.session.add(CommentCode(
                        expert_id=expert_id, post_id=post.post_id,
                        comment_index=comment_index, code=code,
                    ))

        for span in data.get("spans", []):
            existing = SpanAnnotation.query.filter_by(
                expert_id=expert_id, post_id=post.post_id,
                item_index=comment_index,
                start_offset=span["start"], end_offset=span["end"],
            ).first()
            if not existing:
                db.session.add(SpanAnnotation(
                    expert_id=expert_id, post_id=post.post_id,
                    item_index=comment_index,
                    start_offset=span["start"], end_offset=span["end"],
                    highlighted_text=span["text"],
                    span_type=span.get("span_type", "CLAIM"),
                ))

    db.session.add(PreSeedLog(expert_id=expert_id, post_id=post.post_id))
    db.session.commit()


# ── Project List ─────────────────────────────────────

@app.route("/")
def index():
    projects = Project.query.order_by(Project.created_at.desc()).all()
    project_stats = {}
    for p in projects:
        post_count = Post.query.filter_by(project_id=p.id).count()
        expert_count = Expert.query.filter_by(project_id=p.id).filter(Expert.username != "admin").count()
        project_stats[p.id] = {"posts": post_count, "experts": expert_count}
    return render_template("projects.html", projects=projects, project_stats=project_stats)


@app.route("/projects/create", methods=["POST"])
def create_project():
    password = request.form.get("admin_password", "")
    if password != GLOBAL_ADMIN_PASSWORD:
        return "Invalid admin password", 403

    name = request.form.get("name", "").strip()
    if not name:
        return "Project name required", 400

    csv_file = request.files.get("csv_file")
    if not csv_file:
        return "CSV file required", 400

    link_template = request.form.get("link_template", "").strip()
    project_admin_pw = request.form.get("project_admin_password", "admin123").strip() or "admin123"

    try:
        content = csv_file.read().decode("utf-8-sig")
        posts_data, config = parse_csv_content(content, link_template)
    except Exception as e:
        return f"CSV parsing error: {e}", 400

    if not posts_data:
        return "No posts found in CSV", 400

    slug = slugify(name)
    project = Project(name=name, slug=slug)
    project.set_config(config)
    db.session.add(project)
    db.session.flush()

    for i, (pid, pdata) in enumerate(posts_data.items()):
        post = Post(
            project_id=project.id,
            post_id=pid,
            title=pdata["title"],
            body=pdata["body"],
            labels=pdata["labels"],
            split=pdata["split"],
            link=pdata["link"],
            order_index=i,
            pre_annotations_json=json.dumps(pdata["pre_annotations"]),
        )
        db.session.add(post)
        db.session.flush()

        for j, c in enumerate(pdata["comments"]):
            db.session.add(Comment(
                post_ref_id=post.id,
                comment_index=j,
                comment_id=c["comment_id"],
                comment_body=c["comment_body"],
            ))

    admin = Expert(project_id=project.id, username="admin")
    admin.set_password(project_admin_pw)
    admin.password_plain = project_admin_pw
    db.session.add(admin)
    db.session.commit()

    total_posts = len(posts_data)
    total_comments = sum(len(p["comments"]) for p in posts_data.values())
    total_codes = sum(
        sum(len(d.get("codes", [])) for d in p["pre_annotations"].values())
        for p in posts_data.values()
    )
    total_spans = sum(
        sum(len(d.get("spans", [])) for d in p["pre_annotations"].values())
        for p in posts_data.values()
    )
    logging.info(
        f"[create] Project '{name}' ({slug}): {total_posts} posts, "
        f"{total_comments} comments, {total_codes} pre-annotated codes, {total_spans} pre-annotated spans"
    )

    session["expert_id"] = admin.id
    session["username"] = "admin"
    session["project_slug"] = slug
    return redirect(url_for("admin", slug=slug))


@app.route("/projects/<int:project_id>/delete", methods=["POST"])
def delete_project(project_id):
    password = request.form.get("admin_password", "")
    if password != GLOBAL_ADMIN_PASSWORD:
        return "Invalid admin password", 403

    project = Project.query.get(project_id)
    if not project:
        return "Project not found", 404

    # Delete all related data
    expert_ids = [e.id for e in Expert.query.filter_by(project_id=project.id).all()]
    if expert_ids:
        CommentCode.query.filter(CommentCode.expert_id.in_(expert_ids)).delete(synchronize_session=False)
        SpanAnnotation.query.filter(SpanAnnotation.expert_id.in_(expert_ids)).delete(synchronize_session=False)
        Assignment.query.filter(Assignment.expert_id.in_(expert_ids)).delete(synchronize_session=False)
        PreSeedLog.query.filter(PreSeedLog.expert_id.in_(expert_ids)).delete(synchronize_session=False)
    Expert.query.filter_by(project_id=project.id).delete()
    # Posts and Comments cascade via relationship
    db.session.delete(project)
    db.session.commit()
    return redirect(url_for("index"))


# ── Auth ─────────────────────────────────────────────

@app.route("/p/<slug>/login", methods=["GET", "POST"])
def login(slug):
    project = get_project(slug)
    if not project:
        return "Project not found", 404

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        if not username or not password:
            return render_template("login.html", project=project, error="Username and password required")
        expert = Expert.query.filter_by(project_id=project.id, username=username).first()
        if not expert:
            return render_template("login.html", project=project, error="Invalid username or password")
        if not expert.password_hash:
            expert.set_password(password)
            db.session.commit()
        elif not expert.check_password(password):
            return render_template("login.html", project=project, error="Invalid username or password")
        session["expert_id"] = expert.id
        session["username"] = expert.username
        session["project_slug"] = slug
        return redirect(url_for("dashboard", slug=slug))

    return render_template("login.html", project=project)


@app.route("/p/<slug>/register", methods=["GET", "POST"])
def register(slug):
    project = get_project(slug)
    if not project:
        return "Project not found", 404

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        confirm = request.form.get("confirm_password", "").strip()
        if not username or not password:
            return render_template("register.html", project=project, error="Username and password are required")
        if len(username) < 3:
            return render_template("register.html", project=project, error="Username must be at least 3 characters")
        if len(password) < 6:
            return render_template("register.html", project=project, error="Password must be at least 6 characters")
        if password != confirm:
            return render_template("register.html", project=project, error="Passwords do not match")
        if Expert.query.filter_by(project_id=project.id, username=username).first():
            return render_template("register.html", project=project, error="Username already taken")
        new_expert = Expert(project_id=project.id, username=username)
        new_expert.set_password(password)
        db.session.add(new_expert)
        db.session.commit()
        session["expert_id"] = new_expert.id
        session["username"] = new_expert.username
        session["project_slug"] = slug
        return redirect(url_for("dashboard", slug=slug))

    return render_template("register.html", project=project)


@app.route("/p/<slug>/logout")
def logout(slug):
    session.clear()
    return redirect(url_for("login", slug=slug))


# ── Dashboard ────────────────────────────────────────

@app.route("/p/<slug>/")
def dashboard(slug):
    project = get_project(slug)
    if not project:
        return "Project not found", 404

    expert = get_expert()
    if not expert or expert.project_id != project.id:
        return redirect(url_for("login", slug=slug))

    search = request.args.get("search", "").strip()
    is_admin = expert.username == "admin"
    post_ids = get_post_ids_for_project(project)

    if not is_admin:
        assigned_ids = {a.post_id for a in Assignment.query.filter_by(expert_id=expert.id).all()}
        if not assigned_ids:
            return render_template("dashboard.html", project=project, posts=[], search=search,
                                   username=session.get("username"), is_admin=False,
                                   total_comments=0, total_coded=0)

    posts_list = []
    for pid in post_ids:
        if not is_admin and pid not in assigned_ids:
            continue
        post = get_post_by_ext_id(project, pid)
        if not post:
            continue
        post_data = get_post_data(post)
        if search and search.lower() not in (post_data["title"] or "").lower() and search.lower() not in pid.lower():
            continue

        if is_admin:
            coded_count = db.session.query(
                db.func.count(db.func.distinct(CommentCode.comment_index))
            ).filter_by(post_id=pid).join(Expert).filter(Expert.project_id == project.id).scalar() or 0
            assigned_set = {r.username for r in db.session.query(Expert.username)
                            .join(Assignment, Expert.id == Assignment.expert_id)
                            .filter(Assignment.post_id == pid, Expert.project_id == project.id).all()}
            coded_set = {r.username for r in db.session.query(Expert.username)
                         .join(CommentCode, Expert.id == CommentCode.expert_id)
                         .filter(CommentCode.post_id == pid, Expert.project_id == project.id).distinct().all()}
            assigned_names = sorted(assigned_set | coded_set)
        else:
            coded_count = db.session.query(
                db.func.count(db.func.distinct(CommentCode.comment_index))
            ).filter_by(expert_id=expert.id, post_id=pid).scalar() or 0
            assigned_names = []

        posts_list.append({
            "post_id": pid,
            "title": post_data["title"],
            "labels": post_data.get("labels", ""),
            "num_comments": len(post_data["advice"]),
            "coded_comments": coded_count,
            "assigned_names": assigned_names,
            "link": post_data["link"],
        })

    posts_list.sort(key=lambda p: -p["num_comments"])
    total_comments = sum(p["num_comments"] for p in posts_list)
    total_coded = sum(p["coded_comments"] for p in posts_list)

    return render_template("dashboard.html", project=project, posts=posts_list, search=search,
                           username=session.get("username"), is_admin=is_admin,
                           total_comments=total_comments, total_coded=total_coded)


# ── Review Page ──────────────────────────────────────

@app.route("/p/<slug>/review/<post_id>")
def review(slug, post_id):
    project = get_project(slug)
    if not project:
        return "Project not found", 404

    expert = get_expert()
    if not expert or expert.project_id != project.id:
        return redirect(url_for("login", slug=slug))

    if expert.username != "admin":
        assigned_ids = {a.post_id for a in Assignment.query.filter_by(expert_id=expert.id).all()}
        if post_id not in assigned_ids:
            return "Not assigned to this post", 403

    post = get_post_by_ext_id(project, post_id)
    if not post:
        return "Post not found", 404

    preseed_for_expert(expert.id, post)
    post_data = get_post_data(post)
    config = project.get_config()
    comment_codes = config.get("comment_codes", {})
    span_types = config.get("span_types", [])

    existing_annotations = []
    for a in SpanAnnotation.query.filter_by(expert_id=expert.id, post_id=post_id).all():
        existing_annotations.append({
            "id": a.id,
            "item_index": a.item_index,
            "start": a.start_offset,
            "end": a.end_offset,
            "text": a.highlighted_text,
            "span_type": a.span_type or "CLAIM",
        })

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
        nav_ids = [pid for pid in get_post_ids_for_project(project) if pid in assigned_ids]
    else:
        nav_ids = get_post_ids_for_project(project)
    try:
        idx = nav_ids.index(post_id)
    except ValueError:
        idx = 0
    prev_id = nav_ids[idx - 1] if idx > 0 else None
    next_id = nav_ids[idx + 1] if idx < len(nav_ids) - 1 else None

    # Pre-hedged indices (any span_type that has pre-annotations but is_span_code)
    pre = post.get_pre_annotations()
    pre_hedged = set()
    for ci, data in pre.items():
        for st in span_types:
            if st in data.get("codes", []) and st != span_types[0] if span_types else False:
                # Mark as "needs span" for non-primary span types that have codes but maybe no spans
                pre_hedged.add(ci)

    comment_data = {"body": post.body, "title": post.title}

    return render_template("review.html", project=project, post=post_data, comment_data=comment_data,
                           existing_annotations=existing_annotations,
                           codes_by_comment=codes_by_comment,
                           exclude_reasons=exclude_reasons,
                           comment_codes=comment_codes,
                           span_types=span_types,
                           pre_hedged=sorted(pre_hedged),
                           prev_id=prev_id, next_id=next_id,
                           username=session.get("username"))


# ── API: Save annotation ────────────────────────────

@app.route("/p/<slug>/api/annotation/<post_id>/save", methods=["POST"])
def save_annotation(slug, post_id):
    expert = get_expert()
    if not expert:
        return jsonify({"error": "Not logged in"}), 401

    data = request.json
    item_index = data.get("item_index", 0)
    start = data["start"]
    end = data["end"]

    overlapping = SpanAnnotation.query.filter_by(
        expert_id=expert.id, post_id=post_id, item_index=item_index,
    ).filter(
        SpanAnnotation.start_offset < end,
        SpanAnnotation.end_offset > start,
    ).all()
    removed_ids = [a.id for a in overlapping]
    for a in overlapping:
        db.session.delete(a)

    annot = SpanAnnotation(
        expert_id=expert.id, post_id=post_id, item_index=item_index,
        start_offset=start, end_offset=end,
        highlighted_text=data["text"],
        span_type=data.get("span_type", "CLAIM"),
    )
    db.session.add(annot)
    db.session.commit()
    return jsonify({"ok": True, "id": annot.id, "removed_ids": removed_ids, "span_type": annot.span_type})


@app.route("/p/<slug>/api/codes/<post_id>/save", methods=["POST"])
def save_codes(slug, post_id):
    project = get_project(slug)
    expert = get_expert()
    if not expert:
        return jsonify({"error": "Not logged in"}), 401
    data = request.json
    comment_index = data.get("comment_index")
    codes = data.get("codes", [])
    exclude_reason = data.get("exclude_reason", "")

    config = project.get_config()
    valid_codes = config.get("comment_codes", {})

    CommentCode.query.filter_by(
        expert_id=expert.id, post_id=post_id, comment_index=comment_index
    ).delete()

    for code in codes:
        if code in valid_codes:
            db.session.add(CommentCode(
                expert_id=expert.id, post_id=post_id,
                comment_index=comment_index, code=code,
                reason=exclude_reason if code == "EXCLUDE" else "",
            ))
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/p/<slug>/api/annotation/<int:annot_id>/delete", methods=["POST"])
def delete_annotation(slug, annot_id):
    expert = get_expert()
    if not expert:
        return jsonify({"error": "Not logged in"}), 401
    annot = SpanAnnotation.query.get(annot_id)
    if annot and annot.expert_id == expert.id:
        db.session.delete(annot)
        db.session.commit()
    return jsonify({"ok": True})


# ── Admin ────────────────────────────────────────────

@app.route("/p/<slug>/admin", methods=["GET"])
def admin(slug):
    project, expert, err = require_project_admin(slug)
    if err:
        return err

    config = project.get_config()
    experts = Expert.query.filter_by(project_id=project.id).all()
    post_ids = get_post_ids_for_project(project)

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
    for pid in post_ids:
        post = get_post_by_ext_id(project, pid)
        if post:
            comment_count = Comment.query.filter_by(post_ref_id=post.id).count()
            post_meta[pid] = {"num_comments": comment_count, "split": post.split}

    expert_stats = {}
    for e in experts:
        if e.username == "admin":
            continue
        total_comments = 0
        for pid in assignments.get(e.id, []):
            post = get_post_by_ext_id(project, pid)
            if post:
                total_comments += Comment.query.filter_by(post_ref_id=post.id).count()
        coded_comments = db.session.query(
            db.func.count(db.func.distinct(
                CommentCode.post_id + '_' + db.cast(CommentCode.comment_index, db.String)
            ))
        ).filter_by(expert_id=e.id).scalar() or 0
        expert_stats[e.id] = {
            "total_comments": total_comments,
            "coded_comments": coded_comments,
        }

    return render_template("admin.html", project=project, experts=experts, assignments=assignments,
                           taken_by=taken_by, post_meta=post_meta,
                           expert_stats=expert_stats,
                           all_post_ids=post_ids, username=session.get("username"))


@app.route("/p/<slug>/admin/add_expert", methods=["POST"])
def admin_add_expert(slug):
    project, expert, err = require_project_admin(slug)
    if err:
        return err
    data = request.json
    username = data.get("username", "").strip()
    password = data.get("password", "").strip()
    if not username or not password:
        return jsonify({"error": "Username and password required"}), 400
    if Expert.query.filter_by(project_id=project.id, username=username).first():
        return jsonify({"error": "Username already exists"}), 400
    new_expert = Expert(project_id=project.id, username=username)
    new_expert.set_password(password)
    new_expert.password_plain = password
    db.session.add(new_expert)
    db.session.commit()
    return jsonify({"ok": True, "id": new_expert.id})


@app.route("/p/<slug>/admin/delete_expert", methods=["POST"])
def admin_delete_expert(slug):
    project, expert, err = require_project_admin(slug)
    if err:
        return err
    data = request.json
    expert_id = data.get("expert_id")
    target = Expert.query.get(expert_id)
    if not target or target.project_id != project.id:
        return jsonify({"error": "Expert not found"}), 404
    if target.username == "admin":
        return jsonify({"error": "Cannot delete admin"}), 400
    CommentCode.query.filter_by(expert_id=expert_id).delete()
    SpanAnnotation.query.filter_by(expert_id=expert_id).delete()
    Assignment.query.filter_by(expert_id=expert_id).delete()
    PreSeedLog.query.filter_by(expert_id=expert_id).delete()
    db.session.delete(target)
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/p/<slug>/admin/assign", methods=["POST"])
def admin_assign(slug):
    project, expert, err = require_project_admin(slug)
    if err:
        return err
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


@app.route("/p/<slug>/admin/reset_preseed", methods=["POST"])
def admin_reset_preseed(slug):
    project, expert, err = require_project_admin(slug)
    if err:
        return err
    expert_ids = [e.id for e in Expert.query.filter_by(project_id=project.id).all()]
    deleted = PreSeedLog.query.filter(PreSeedLog.expert_id.in_(expert_ids)).delete(synchronize_session=False) if expert_ids else 0
    db.session.commit()
    return jsonify({"ok": True, "deleted": deleted})


# ── Admin: DB Viewer ─────────────────────────────────

@app.route("/p/<slug>/admin/db")
def admin_db(slug):
    project, expert, err = require_project_admin(slug)
    if err:
        return err

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

    return render_template("admin_db.html", project=project,
                           tables=table_names, table_counts=table_counts,
                           selected=selected, columns=columns, rows=rows,
                           row_count=row_count,
                           limit=int(request.args.get("limit", 100)),
                           offset=int(request.args.get("offset", 0)),
                           username=session.get("username"))


# ── Admin: Export CSV ────────────────────────────────

@app.route("/p/<slug>/admin/export_csv")
def admin_export_csv(slug):
    project, expert, err = require_project_admin(slug)
    if err:
        return err

    expert_ids = [e.id for e in Expert.query.filter_by(project_id=project.id).all()]

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "annotation_id", "expert", "post_id", "comment_index",
        "span_type", "highlighted_span", "start_offset", "end_offset", "created_at"
    ])

    for a in SpanAnnotation.query.filter(SpanAnnotation.expert_id.in_(expert_ids)).order_by(
            SpanAnnotation.post_id, SpanAnnotation.item_index, SpanAnnotation.start_offset).all():
        expert_obj = Expert.query.get(a.expert_id)
        writer.writerow([
            a.id, expert_obj.username if expert_obj else "unknown",
            a.post_id, a.item_index, a.span_type or "CLAIM",
            a.highlighted_text, a.start_offset, a.end_offset,
            a.created_at.isoformat() if a.created_at else "",
        ])

    codes_output = io.StringIO()
    codes_writer = csv.writer(codes_output)
    codes_writer.writerow(["expert", "post_id", "comment_index", "code", "exclude_reason"])
    for cc in CommentCode.query.filter(CommentCode.expert_id.in_(expert_ids)).order_by(
            CommentCode.post_id, CommentCode.comment_index).all():
        expert_obj = Expert.query.get(cc.expert_id)
        codes_writer.writerow([
            expert_obj.username if expert_obj else "unknown",
            cc.post_id, cc.comment_index, cc.code, cc.reason or "",
        ])

    combined = output.getvalue() + "\n\n--- COMMENT CODES ---\n" + codes_output.getvalue()
    return Response(combined, mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={project.slug}_annotations.csv"})


# ── Admin: Review all annotations on a post ──────────

@app.route("/p/<slug>/admin/review/<post_id>")
def admin_review(slug, post_id):
    project, expert, err = require_project_admin(slug)
    if err:
        return err

    post = get_post_by_ext_id(project, post_id)
    if not post:
        return "Post not found", 404

    post_data = get_post_data(post)
    config = project.get_config()
    comment_codes = config.get("comment_codes", {})
    comment_data = {"body": post.body, "title": post.title}

    expert_ids = [e.id for e in Expert.query.filter_by(project_id=project.id).all()]

    all_annotations = []
    annotations_by_expert = {}
    for a in SpanAnnotation.query.filter(
            SpanAnnotation.expert_id.in_(expert_ids),
            SpanAnnotation.post_id == post_id).all():
        expert_obj = Expert.query.get(a.expert_id)
        annot = {
            "id": a.id,
            "expert_name": expert_obj.username if expert_obj else "unknown",
            "item_index": a.item_index,
            "start": a.start_offset,
            "end": a.end_offset,
            "text": a.highlighted_text,
            "span_type": a.span_type or "CLAIM",
        }
        all_annotations.append(annot)
        annotations_by_expert.setdefault(annot["expert_name"], []).append(annot)

    post_ids = get_post_ids_for_project(project)
    try:
        idx = post_ids.index(post_id)
    except ValueError:
        idx = 0
    prev_id = post_ids[idx - 1] if idx > 0 else None
    next_id = post_ids[idx + 1] if idx < len(post_ids) - 1 else None

    all_codes = {}
    for cc in CommentCode.query.filter(
            CommentCode.expert_id.in_(expert_ids),
            CommentCode.post_id == post_id).all():
        expert_obj = Expert.query.get(cc.expert_id)
        name = expert_obj.username if expert_obj else "unknown"
        key = f"{name}_{cc.comment_index}"
        if key not in all_codes:
            all_codes[key] = {"expert_name": name, "comment_index": cc.comment_index, "codes": [], "exclude_reason": ""}
        all_codes[key]["codes"].append(cc.code)
        if cc.code == "EXCLUDE" and cc.reason:
            all_codes[key]["exclude_reason"] = cc.reason

    return render_template("admin_review.html", project=project, post=post_data,
                           comment_data=comment_data,
                           all_annotations=all_annotations,
                           annotations_by_expert=annotations_by_expert,
                           expert_names=sorted(annotations_by_expert.keys()),
                           all_codes=list(all_codes.values()),
                           comment_codes=comment_codes,
                           prev_id=prev_id, next_id=next_id,
                           username=session.get("username"))


# ── Health ───────────────────────────────────────────

@app.route("/health")
def health():
    try:
        project_count = Project.query.count()
        return jsonify({"status": "ok", "projects": project_count,
                        "db_uri": "postgresql" if "postgresql" in app.config["SQLALCHEMY_DATABASE_URI"] else "sqlite"})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


# ── Startup ──────────────────────────────────────────

with app.app_context():
    db_uri = app.config["SQLALCHEMY_DATABASE_URI"]
    logging.info(f"[startup] DB type: {'postgresql' if 'postgresql' in db_uri else 'sqlite'}")
    try:
        db.create_all()
        logging.info("[startup] Tables created/verified")
        project_count = Project.query.count()
        logging.info(f"[startup] {project_count} projects loaded")
    except Exception as e:
        logging.error(f"[startup] FATAL: {e}")
        raise


if __name__ == "__main__":
    app.run(debug=True, port=5005, use_reloader=False)
