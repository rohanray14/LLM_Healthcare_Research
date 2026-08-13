import csv
from pathlib import Path
from collections import OrderedDict

BASE = Path(__file__).resolve().parent / "data"


def _load_csv(path):
    grouped = OrderedDict()
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pid = (row.get("post_id") or "").strip()
            if not pid:
                continue
            if pid not in grouped:
                grouped[pid] = {
                    "title": (row.get("post_title") or "").strip(),
                    "body": (row.get("post_body") or "").strip(),
                    "comments": [],
                }
            grouped[pid]["comments"].append({
                "comment_id": (row.get("comment_id") or "").strip(),
                "comment_body": (row.get("comment_body") or "").strip(),
            })
    return grouped


def load_all():
    posts = {}
    comments = {}

    path = BASE / "comments.csv"
    if not path.exists():
        return [], {}, {}

    grouped = _load_csv(path)

    seen = set()
    post_ids = []

    for pid, info in grouped.items():
        advice_items = []
        for c in info["comments"]:
            advice_items.append({
                "advice": c["comment_body"],
                "comment_id": c["comment_id"],
            })

        if not advice_items:
            continue

        posts[pid] = {
            "post_id": pid,
            "title": info["title"],
            "body": info["body"],
            "link": f"https://www.reddit.com/r/methadone/comments/{pid}/",
            "advice": advice_items,
        }

        comments[pid] = {
            "post_id": pid,
            "title": info["title"],
            "body": info["body"],
        }

        if pid not in seen:
            seen.add(pid)
            post_ids.append(pid)

    return post_ids, posts, comments
