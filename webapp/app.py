from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
import json

from config import SECRET_KEY, USERS, LABELS
from models import (
    init_db, posts_loaded, get_posts, get_post, get_llm_outputs,
    get_adjacent_posts, get_existing_verifications, save_verification, get_progress,
)

app = Flask(__name__)
app.secret_key = SECRET_KEY

# Auto-initialize on import (for gunicorn)
init_db()
if not posts_loaded():
    from load_data import load_6k_data, load_llm_outputs
    load_6k_data()
    load_llm_outputs()


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "username" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        if USERS.get(username) == password:
            session["username"] = username
            return redirect(url_for("dashboard"))
        flash("Invalid credentials", "danger")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@login_required
def dashboard():
    label = request.args.get("label", "")
    status = request.args.get("status", "")
    search = request.args.get("search", "")
    page = int(request.args.get("page", 1))

    posts, total = get_posts(
        label_filter=label or None,
        verified_filter=status or None,
        search=search or None,
        page=page,
        per_page=25,
        username=session["username"],
    )
    progress = get_progress()
    total_pages = max(1, (total + 24) // 25)

    return render_template(
        "dashboard.html",
        posts=posts,
        total=total,
        page=page,
        total_pages=total_pages,
        labels=LABELS,
        current_label=label,
        current_status=status,
        current_search=search,
        progress=progress,
    )


@app.route("/post/<post_id>")
@login_required
def post_detail(post_id):
    post, comments = get_post(post_id)
    if not post:
        flash("Post not found", "danger")
        return redirect(url_for("dashboard"))

    llm_outputs = get_llm_outputs(post_id)
    label_v, comment_vs = get_existing_verifications(post_id, session["username"])
    prev_id, next_id = get_adjacent_posts(post_id)

    # Parse LLM JSON fields for display
    parsed_llm = []
    for out in llm_outputs:
        parsed = dict(out)
        for field in ["unique_advice_json", "divergences_json", "clinically_relevant_notes_json"]:
            val = out[field]
            if val:
                try:
                    parsed[field + "_parsed"] = json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    parsed[field + "_parsed"] = None
            else:
                parsed[field + "_parsed"] = None
        parsed_llm.append(parsed)

    return render_template(
        "post_detail.html",
        post=post,
        comments=comments,
        llm_outputs=parsed_llm,
        label_v=label_v,
        comment_vs=comment_vs,
        prev_id=prev_id,
        next_id=next_id,
        labels=LABELS,
    )


@app.route("/post/<post_id>/verify", methods=["POST"])
@login_required
def verify_post(post_id):
    label_data = {
        "label1_correct": 1 if request.form.get("label1_correct") else 0,
        "label2_correct": 1 if request.form.get("label2_correct") else 0,
        "label3_correct": 1 if request.form.get("label3_correct") else 0,
        "suggested_label": request.form.get("suggested_label", ""),
        "notes": request.form.get("verification_notes", ""),
    }

    comment_flags = {}
    for key in request.form:
        if key.startswith("flag_"):
            idx = int(key.split("_")[1])
            comment_flags[idx] = {
                "flag": request.form.get(f"flag_{idx}"),
                "note": request.form.get(f"note_{idx}", ""),
            }

    save_verification(post_id, session["username"], label_data, comment_flags)
    flash("Verification saved!", "success")
    return redirect(url_for("post_detail", post_id=post_id))


@app.route("/api/progress")
@login_required
def api_progress():
    return jsonify(get_progress())


@app.route("/export")
@login_required
def export_data():
    import csv
    from io import StringIO
    from flask import Response
    from models import get_db

    conn = get_db()

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["post_id", "expert", "label1_correct", "label2_correct", "label3_correct", "suggested_label", "notes", "created_at"])
    for row in conn.execute("SELECT * FROM label_verifications ORDER BY post_id"):
        writer.writerow([row["post_id"], row["expert_username"], row["label1_correct"], row["label2_correct"], row["label3_correct"], row["suggested_label"], row["notes"], row["created_at"]])

    writer.writerow([])
    writer.writerow(["post_id", "comment_index", "expert", "flag", "note", "created_at"])
    for row in conn.execute("SELECT * FROM comment_verifications ORDER BY post_id, comment_index"):
        writer.writerow([row["post_id"], row["comment_index"], row["expert_username"], row["flag"], row["note"], row["created_at"]])

    conn.close()

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=verifications_export.csv"},
    )


if __name__ == "__main__":
    init_db()
    if not posts_loaded():
        print("Database empty. Loading data from Excel files...")
        from load_data import load_6k_data, load_llm_outputs
        load_6k_data()
        load_llm_outputs()
    app.run(debug=True, port=5001)
