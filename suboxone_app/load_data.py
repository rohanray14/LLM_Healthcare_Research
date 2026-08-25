import csv
from pathlib import Path
from collections import OrderedDict

BASE = Path(__file__).resolve().parent / "data"


def _load_csv(path):
    grouped = OrderedDict()
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pid = (row.get("postId") or "").strip()
            if not pid:
                continue
            if pid not in grouped:
                grouped[pid] = {
                    "title": (row.get("title") or "").strip(),
                    "body": (row.get("body") or "").strip(),
                    "labels": (row.get("labels") or "").strip(),
                    "comments": [],
                }
            comment_text = (row.get("comment") or "").strip()
            l1_coding = (row.get("l1_coding") or "").strip()
            span_if_claim = (row.get("span_if_claim") or "").strip()

            grouped[pid]["comments"].append({
                "comment_id": (row.get("comment_id") or "").strip(),
                "comment_body": comment_text,
                "l1_coding": l1_coding,
                "span_if_claim": span_if_claim,
            })
    return grouped


def _find_span_offsets(comment_text, span_text):
    """Find start/end offsets of span_text within comment_text."""
    idx = comment_text.find(span_text)
    if idx >= 0:
        return idx, idx + len(span_text)
    # Try case-insensitive or trimmed match
    lower_comment = comment_text.lower()
    lower_span = span_text.lower()
    idx = lower_comment.find(lower_span)
    if idx >= 0:
        return idx, idx + len(span_text)
    return None, None


def load_all():
    posts = {}
    comments = {}
    pre_annotations = {}  # {post_id: {comment_index: {codes: [...], spans: [...]}}}

    seen = set()
    post_ids = []

    for csv_file in sorted(BASE.glob("*.csv")):
        grouped = _load_csv(csv_file)
        for pid, info in grouped.items():
            if pid in posts:
                # Skip duplicates across files
                continue

            advice_items = []
            pre_annotations[pid] = {}

            for i, c in enumerate(info["comments"]):
                advice_items.append({
                    "advice": c["comment_body"],
                    "comment_id": c["comment_id"],
                })

                # Parse l1_coding
                codes = []
                if c["l1_coding"]:
                    codes = [code.strip() for code in c["l1_coding"].split(",") if code.strip()]

                # Parse span_if_claim (newline-separated spans)
                spans = []
                if c["span_if_claim"]:
                    span_parts = [s.strip() for s in c["span_if_claim"].split("\n") if s.strip()]
                    for span_text in span_parts:
                        start, end = _find_span_offsets(c["comment_body"], span_text)
                        if start is not None:
                            spans.append({
                                "start": start,
                                "end": end,
                                "text": c["comment_body"][start:end],
                                "span_type": "CLAIM",
                            })

                if codes or spans:
                    pre_annotations[pid][i] = {"codes": codes, "spans": spans}

            if not advice_items:
                continue

            posts[pid] = {
                "post_id": pid,
                "title": info["title"],
                "body": info["body"],
                "labels": info["labels"],
                "link": f"https://www.reddit.com/r/suboxone/comments/{pid}/",
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

    return post_ids, posts, comments, pre_annotations
