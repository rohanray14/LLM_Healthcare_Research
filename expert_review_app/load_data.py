import csv
import json
import openpyxl
from pathlib import Path

BASE = Path(__file__).resolve().parent / "data"
SAMPLE_ANNOT_MODEL = "comment_annotations"


def _safe_json(val):
    if not val:
        return []
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return []


def load_posts():
    """Return dict keyed by (post_id, model_name) with all LLM output fields."""
    wb = openpyxl.load_workbook(BASE / "PostLevel_Outputs.xlsx", read_only=True)
    ws = wb[wb.sheetnames[0]]
    rows = list(ws.iter_rows(values_only=True))
    headers = rows[0]
    posts = {}
    for row in rows[1:]:
        d = dict(zip(headers, row))
        key = (d["post_id"], d["model_name"])
        posts[key] = {
            "class_label": d["class_label"],
            "post_id": d["post_id"],
            "title": d["title"],
            "link": d["link"],
            "model_family": d["model_family"],
            "model_name": d["model_name"],
            "summary": d["summary"] or "",
            "advice": _safe_json(d["unique_advice_json"]),
            "divergences": _safe_json(d["divergences_json"]),
            "clinical_notes": _safe_json(d["clinically_relevant_notes_json"]),
            "data_quality": d["data_quality"] or "",
        }
    wb.close()
    return posts


def load_comments():
    """Return dict keyed by post_id with title, body, and list of comments."""
    wb = openpyxl.load_workbook(BASE / "6K_data_with_comments.xlsx", read_only=True)
    ws = wb[wb.sheetnames[0]]
    rows = list(ws.iter_rows(values_only=True))
    headers = rows[0]
    comment_cols = [h for h in headers if h and h.startswith("Comment")]
    data = {}
    for row in rows[1:]:
        d = dict(zip(headers, row))
        pid = d["id"]
        comments = []
        for col in comment_cols:
            val = d.get(col)
            if val and str(val).strip():
                comments.append(str(val).strip())
        data[pid] = {
            "post_id": pid,
            "title": d.get("title", ""),
            "body": d.get("body", ""),
            "label1": d.get("Label1", ""),
            "label2": d.get("Label2", ""),
            "label3": d.get("Label3", ""),
            "comments": comments,
        }
    wb.close()
    return data


def load_sample_annotations():
    """Load comment-level annotations from sample_annotations.csv.

    Returns posts dict entries keyed by (post_id, SAMPLE_ANNOT_MODEL) and
    a dict of comment data keyed by post_id (used as fallback when post
    is not in the 6K comments file).
    """
    path = BASE / "sample_annotations.csv"
    if not path.exists():
        return {}, {}

    # Group rows by post_id
    from collections import OrderedDict
    grouped = OrderedDict()
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pid = row["post_id"]
            if pid not in grouped:
                grouped[pid] = {
                    "title": row.get("post_title") or "",
                    "body": row.get("post_body") or "",
                    "themes": row.get("themes") or "",
                    "comments": [],
                }
            grouped[pid]["comments"].append({
                "comment_id": row.get("comment_id") or "",
                "comment_body": row.get("comment_body") or "",
                "l1_coding": row.get("l1_coding") or "",
                "span_if_claim": row.get("span_if_claim") or "",
                "nikil_notes": row.get("nikil_notes") or "",
            })

    posts = {}
    comment_fallbacks = {}
    for pid, info in grouped.items():
        # Build advice-style items from each comment annotation
        advice_items = []
        for c in info["comments"]:
            advice_items.append({
                "advice": c["comment_body"],
                "agreement": c["l1_coding"],
                "support": [s.strip() for s in c["span_if_claim"].split(",") if s.strip()] if c["span_if_claim"] else [],
                "counterpoints": [c["nikil_notes"]] if c["nikil_notes"] else [],
            })

        key = (pid, SAMPLE_ANNOT_MODEL)
        posts[key] = {
            "class_label": info["themes"],
            "post_id": pid,
            "title": info["title"],
            "link": f"https://www.reddit.com/r/suboxone/comments/{pid}/",
            "model_family": "sample",
            "model_name": SAMPLE_ANNOT_MODEL,
            "summary": info["body"],
            "advice": advice_items,
            "divergences": [],
            "clinical_notes": [],
            "data_quality": "",
        }

        # Fallback comment data (in case post isn't in 6K file)
        comment_fallbacks[pid] = {
            "post_id": pid,
            "title": info["title"],
            "body": info["body"],
            "label1": "",
            "label2": "",
            "label3": "",
            "comments": [c["comment_body"] for c in info["comments"]],
        }

    return posts, comment_fallbacks


def load_all():
    """Load and merge posts with their comments. Returns list of unique post_ids and full data."""
    posts = load_posts()
    comments = load_comments()

    # Load sample annotations
    sample_posts, sample_comments = load_sample_annotations()
    posts.update(sample_posts)
    # Only add comment fallbacks for posts not already in 6K data
    for pid, cdata in sample_comments.items():
        if pid not in comments:
            comments[pid] = cdata

    # Get unique post_ids preserving order
    seen = set()
    post_ids = []
    for (pid, _) in posts:
        if pid not in seen:
            seen.add(pid)
            post_ids.append(pid)

    # Get model names
    models = sorted(set(m for (_, m) in posts))

    return post_ids, posts, comments, models
