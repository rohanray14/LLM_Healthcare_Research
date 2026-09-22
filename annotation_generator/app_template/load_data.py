import csv
import json
from pathlib import Path
from collections import OrderedDict

BASE = Path(__file__).resolve().parent
MODEL_NAME = "comment_annotations"

# In-memory cache: {project_slug: (post_ids, posts, comments)}
_cache = {}


def _load_csv(path, cols):
    grouped = OrderedDict()
    col_pid = cols["post_id"]
    col_title = cols.get("post_title", "")
    col_body = cols.get("post_body", "")
    col_cid = cols.get("comment_id", "")
    col_cbody = cols["comment_body"]
    col_themes = cols.get("themes", "")

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pid = (row.get(col_pid) or "").strip()
            if not pid:
                continue
            if pid not in grouped:
                grouped[pid] = {
                    "title": (row.get(col_title) or "").strip() if col_title else "",
                    "body": (row.get(col_body) or "").strip() if col_body else "",
                    "themes": (row.get(col_themes) or "").strip() if col_themes else "",
                    "comments": [],
                }
            comment_id = (row.get(col_cid) or "").strip() if col_cid else f"{pid}_c{len(grouped[pid]['comments'])}"
            comment_body = (row.get(col_cbody) or "").strip()
            if comment_body:
                grouped[pid]["comments"].append({
                    "comment_id": comment_id,
                    "comment_body": comment_body,
                })
    return grouped


def _build_posts(grouped, config):
    posts = {}
    comments = {}
    link_tpl = config.get("link_template", "")

    for pid, info in grouped.items():
        advice_items = []
        for c in info["comments"]:
            advice_items.append({
                "advice": c["comment_body"],
                "comment_id": c["comment_id"],
                "agreement": "",
                "support": [],
                "counterpoints": [],
                "gt_spans": [],
            })

        if not advice_items:
            continue

        posts[(pid, MODEL_NAME)] = {
            "class_label": info["themes"],
            "post_id": pid,
            "title": info["title"],
            "link": link_tpl.replace("{post_id}", pid) if link_tpl else "",
            "model_family": "sample",
            "model_name": MODEL_NAME,
            "summary": info["body"],
            "advice": advice_items,
            "divergences": [],
            "clinical_notes": [],
            "data_quality": "",
        }

        comments[pid] = {
            "post_id": pid,
            "title": info["title"],
            "body": info["body"],
            "comments": [c["comment_body"] for c in info["comments"]],
        }

    return posts, comments


def load_project(slug, config):
    """Load data for a project. Uses cache."""
    if slug in _cache:
        return _cache[slug]

    data_file = config.get("data_file", "data.csv")
    path = BASE / "data" / slug / data_file
    if not path.exists():
        return [], {}, {}

    grouped = _load_csv(path, config["columns"])
    posts, comments = _build_posts(grouped, config)

    seen = set()
    post_ids = []
    for (pid, _) in posts:
        if pid not in seen:
            seen.add(pid)
            post_ids.append(pid)

    _cache[slug] = (post_ids, posts, comments)
    return post_ids, posts, comments


def clear_cache(slug=None):
    if slug:
        _cache.pop(slug, None)
    else:
        _cache.clear()
