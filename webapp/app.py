import os
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
import json

from config import SECRET_KEY, USERS, LABELS, COMMENT_CODES
from sheets import is_configured as sheets_configured, push_annotations_async, push_annotations, pull_input_data, get_sync_status, get_active_sheet_id, set_sheet_id
from models import (
    init_db, posts_loaded, get_posts, get_post, get_llm_outputs,
    get_adjacent_posts, get_existing_verifications, save_verification, get_progress,
    get_claims, save_claims,
    get_comment_codes, save_comment_codes, get_post_code_summaries,
    get_comment_spans, save_comment_spans, delete_comment_span, get_annotation_progress,
    get_claim_spans_for_review, get_expert_reviews, save_expert_reviews,
    get_posts_with_claims, get_expert_review_progress,
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
        user = USERS.get(username)
        if user and user["password"] == password:
            session["username"] = username
            session["role"] = user["role"]
            if user["role"] == "annotator":
                return redirect(url_for("annotator_dashboard"))
            return redirect(url_for("expert_review_dashboard"))
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
    claims_by_comment = get_claims(post_id, session["username"])
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
        claims_by_comment=claims_by_comment,
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

    # Parse claim annotations
    claims_data = []
    claim_idx = 0
    while True:
        claim_text = request.form.get(f"claim_text_{claim_idx}")
        if claim_text is None:
            break
        if claim_text.strip():
            claims_data.append({
                "comment_index": int(request.form.get(f"claim_comment_{claim_idx}", 0)),
                "claim_text": claim_text,
                "credibility": request.form.get(f"claim_credibility_{claim_idx}", "unclear"),
                "evidence_type": request.form.get(f"claim_evidence_{claim_idx}", "anecdotal"),
                "note": request.form.get(f"claim_note_{claim_idx}", ""),
            })
        claim_idx += 1

    save_verification(post_id, session["username"], label_data, comment_flags)
    save_claims(post_id, session["username"], claims_data)
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

    writer.writerow([])
    writer.writerow(["post_id", "comment_index", "expert", "claim_text", "credibility", "evidence_type", "note", "created_at"])
    for row in conn.execute("SELECT * FROM claim_annotations ORDER BY post_id, comment_index, id"):
        writer.writerow([row["post_id"], row["comment_index"], row["expert_username"], row["claim_text"], row["credibility"], row["evidence_type"], row["note"], row["created_at"]])

    conn.close()

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=verifications_export.csv"},
    )


# ── Annotator (non-expert) routes ──────────────────────────────────

@app.route("/annotate")
@login_required
def annotator_dashboard():
    search = request.args.get("search", "")
    code_filter = request.args.getlist("codes")  # multi-select
    page = int(request.args.get("page", 1))

    posts, total = get_posts(
        search=search or None,
        page=page,
        per_page=25,
        username=session["username"],
    )
    progress = get_annotation_progress(session["username"])
    total_pages = max(1, (total + 24) // 25)

    # Get code summaries for displayed posts
    post_ids = [p["id"] for p in posts]
    code_summaries = get_post_code_summaries(post_ids, session["username"])

    # Filter by codes if requested (client-side would be simpler but let's do server-side)
    if code_filter:
        filtered_posts = []
        for p in posts:
            post_codes = code_summaries.get(p["id"], [])
            if any(c in post_codes for c in code_filter):
                filtered_posts.append(p)
        posts = filtered_posts

    return render_template(
        "annotator_dashboard.html",
        posts=posts,
        total=total,
        page=page,
        total_pages=total_pages,
        current_search=search,
        current_codes=code_filter,
        progress=progress,
        code_summaries=code_summaries,
        comment_codes=COMMENT_CODES,
    )


@app.route("/annotate/<post_id>")
@login_required
def annotate_post(post_id):
    post, comments = get_post(post_id)
    if not post:
        flash("Post not found", "danger")
        return redirect(url_for("annotator_dashboard"))

    codes_by_comment = get_comment_codes(post_id, session["username"])
    spans_by_comment = get_comment_spans(post_id, session["username"])
    prev_id, next_id = get_adjacent_posts(post_id)

    return render_template(
        "annotate_post.html",
        post=post,
        comments=comments,
        codes_by_comment=codes_by_comment,
        spans_by_comment=spans_by_comment,
        prev_id=prev_id,
        next_id=next_id,
        comment_codes=COMMENT_CODES,
    )


@app.route("/annotate/<post_id>/save_all", methods=["POST"])
@login_required
def save_all_annotations(post_id):
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data"}), 400

    # Save comment-level codes: {comment_index: [code1, code2, ...]}
    codes = data.get("codes", {})
    save_comment_codes(post_id, session["username"], codes)

    # Save claim spans
    spans = data.get("spans", [])
    save_comment_spans(post_id, session["username"], spans)

    # Auto-sync to Google Sheets (non-blocking)
    username = session["username"]
    if sheets_configured(username):
        push_annotations_async(username)

    return jsonify({"ok": True, "codes_saved": sum(len(v) for v in codes.values()), "spans_saved": len(spans)})


# ── Expert claim review routes ─────────────────────────────────────

EXPERT_VERDICTS = ["correct", "incorrect", "rumor", "unsure"]


@app.route("/review")
@login_required
def expert_review_dashboard():
    search = request.args.get("search", "")
    page = int(request.args.get("page", 1))

    posts, total = get_posts_with_claims(
        page=page, per_page=25, search=search or None,
    )
    progress = get_expert_review_progress(session["username"])
    total_pages = max(1, (total + 24) // 25)

    return render_template(
        "expert_review_dashboard.html",
        posts=posts,
        total=total,
        page=page,
        total_pages=total_pages,
        current_search=search,
        progress=progress,
    )


@app.route("/review/<post_id>")
@login_required
def review_post(post_id):
    post, comments = get_post(post_id)
    if not post:
        flash("Post not found", "danger")
        return redirect(url_for("expert_review_dashboard"))

    claims_by_comment = get_claim_spans_for_review(post_id)
    existing_reviews = get_expert_reviews(post_id, session["username"])
    prev_id, next_id = get_adjacent_posts(post_id)

    return render_template(
        "review_post.html",
        post=post,
        comments=comments,
        claims_by_comment=claims_by_comment,
        existing_reviews=existing_reviews,
        prev_id=prev_id,
        next_id=next_id,
        verdicts=EXPERT_VERDICTS,
    )


@app.route("/review/<post_id>/save", methods=["POST"])
@login_required
def save_review(post_id):
    data = request.get_json()
    if not data or "reviews" not in data:
        return jsonify({"error": "No data"}), 400

    save_expert_reviews(post_id, session["username"], data["reviews"])

    # Auto-sync to Google Sheets (non-blocking)
    username = session["username"]
    if sheets_configured(username):
        push_annotations_async(username)

    return jsonify({"ok": True, "count": len(data["reviews"])})


# ── Google Sheets sync routes ───────────────────────────────────────

@app.route("/sheets")
@login_required
def sheets_status():
    has_creds = bool(os.environ.get("GOOGLE_SHEETS_CREDENTIALS_JSON"))
    username = session["username"]
    current_sheet_id = get_active_sheet_id(username)
    if not sheets_configured(username):
        return render_template("sheets.html", configured=False, status=None,
                               has_creds=has_creds, current_sheet_id=current_sheet_id or "")
    status = get_sync_status(username)
    return render_template("sheets.html", configured=True, status=status,
                           has_creds=has_creds, current_sheet_id=current_sheet_id or "")


@app.route("/sheets/switch", methods=["POST"])
@login_required
def sheets_switch():
    new_id = request.form.get("sheet_id", "").strip()
    if not new_id:
        flash("Please enter a Google Sheet ID", "danger")
        return redirect(url_for("sheets_status"))
    set_sheet_id(new_id, session["username"])
    flash(f"Switched to sheet: {new_id}", "success")
    return redirect(url_for("sheets_status"))


@app.route("/sheets/push", methods=["POST"])
@login_required
def sheets_push():
    username = session["username"]
    if not sheets_configured(username):
        flash("Google Sheets not configured", "danger")
        return redirect(url_for("sheets_status"))
    ok, msg = push_annotations(username)
    flash(msg, "success" if ok else "danger")
    return redirect(url_for("sheets_status"))


@app.route("/sheets/pull", methods=["POST"])
@login_required
def sheets_pull():
    username = session["username"]
    if not sheets_configured(username):
        flash("Google Sheets not configured", "danger")
        return redirect(url_for("sheets_status"))
    ok, msg = pull_input_data(username)
    flash(msg, "success" if ok else "danger")
    return redirect(url_for("sheets_status"))


@app.route("/reload-excel", methods=["POST"])
@login_required
def reload_excel():
    """Re-sync database from Excel source files.

    Updates existing posts and adds new ones so the DB stays
    in sync with any Excel edits made outside the app.
    """
    from load_data import reload_from_excel
    result = reload_from_excel()
    flash(result["msg"], "success" if result["ok"] else "danger")
    # Also push to Google Sheets so everything stays in sync
    if result["ok"] and sheets_configured(session["username"]):
        push_annotations_async(session["username"])
    return redirect(url_for("sheets_status"))


if __name__ == "__main__":
    init_db()
    if not posts_loaded():
        print("Database empty. Loading data from Excel files...")
        from load_data import load_6k_data, load_llm_outputs
        load_6k_data()
        load_llm_outputs()
    app.run(debug=True, port=5001)
