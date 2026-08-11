import csv
import re
from pathlib import Path
from collections import OrderedDict

BASE = Path(__file__).resolve().parent / "data"
MODEL_NAME = "comment_annotations"


def _load_csv(path):
    """Load a CSV file and group rows by post_id.

    Handles column order differences between dev and test sheets.
    Returns OrderedDict keyed by post_id.
    """
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
                    "themes": (row.get("themes") or "").strip(),
                    "comments": [],
                }
            grouped[pid]["comments"].append({
                "comment_id": (row.get("comment_id") or "").strip(),
                "comment_body": (row.get("comment_body") or "").strip(),
                "l1_coding": (row.get("l1_coding") or "").strip(),
                "span_if_claim": (row.get("span_if_claim") or "").strip(),
                "nikil_notes": (row.get("nikil_notes") or "").strip(),
            })
    return grouped


def _compute_gt_spans(comment_body, span_if_claim):
    """Compute ground-truth span offsets within the comment body."""
    gt_spans = []
    if not span_if_claim:
        return gt_spans

    body = comment_body
    body_clean = re.sub(r'\*{1,2}(.+?)\*{1,2}', r'\1', body)
    raw_spans = span_if_claim.replace("|", ",")

    for span_text in raw_spans.split(","):
        span_text = span_text.strip()
        if not span_text:
            continue

        idx = body.find(span_text)
        if idx == -1:
            idx = body.lower().find(span_text.lower())

        if idx == -1:
            span_ws = re.sub(r'\s+', ' ', span_text).strip()
            for m in re.finditer(re.escape(span_ws[:30]), re.sub(r'\s+', ' ', body)):
                norm_start = m.start()
                orig_i = 0
                norm_i = 0
                while norm_i < norm_start and orig_i < len(body):
                    if body[orig_i].isspace():
                        while orig_i < len(body) and body[orig_i].isspace():
                            orig_i += 1
                        norm_i += 1
                    else:
                        orig_i += 1
                        norm_i += 1
                orig_chunk = re.sub(r'\s+', ' ', body[orig_i:orig_i + len(span_text) + 50])
                if orig_chunk.startswith(span_ws):
                    span_end = orig_i
                    ws_matched = 0
                    while span_end < len(body) and ws_matched < len(span_ws):
                        if body[span_end].isspace():
                            while span_end < len(body) and body[span_end].isspace():
                                span_end += 1
                            ws_matched += 1
                        else:
                            span_end += 1
                            ws_matched += 1
                    gt_spans.append({
                        "text": body[orig_i:span_end],
                        "start": orig_i,
                        "end": span_end,
                    })
                    idx = orig_i
                    break

        if idx == -1:
            idx_clean = body_clean.find(span_text)
            if idx_clean == -1:
                idx_clean = body_clean.lower().find(span_text.lower())
            if idx_clean >= 0:
                orig_pos = 0
                clean_pos = 0
                for ch in body:
                    if clean_pos >= idx_clean:
                        break
                    if body_clean[clean_pos:clean_pos + 1] == ch:
                        clean_pos += 1
                    orig_pos += 1
                idx = orig_pos
                span_end = idx
                clean_matched = 0
                while span_end < len(body) and clean_matched < len(span_text):
                    if body[span_end] in '*_':
                        span_end += 1
                        continue
                    clean_matched += 1
                    span_end += 1
                gt_spans.append({
                    "text": body[idx:span_end],
                    "start": idx,
                    "end": span_end,
                })
                continue

        if idx >= 0:
            gt_spans.append({
                "text": span_text,
                "start": idx,
                "end": idx + len(span_text),
            })

    return gt_spans


def _build_posts_and_comments(grouped, split_name):
    """Convert grouped CSV data into posts and comment dicts."""
    posts = {}
    comments = {}

    # Semantic markers that qualify a comment for expert review
    REQUIRED_MARKERS = {"CLAIM", "HEDGE", "HEDGED", "EXPER"}

    for pid, info in grouped.items():
        advice_items = []
        for c in info["comments"]:
            # Filter: only include comments with at least one required marker
            codes = {t.strip().upper() for t in c["l1_coding"].split(",") if t.strip()}
            if not codes & REQUIRED_MARKERS:
                continue

            gt_spans = _compute_gt_spans(c["comment_body"], c["span_if_claim"])
            advice_items.append({
                "advice": c["comment_body"],
                "comment_id": c["comment_id"],
                "agreement": c["l1_coding"],
                "support": [s.strip() for s in c["span_if_claim"].split(",") if s.strip()] if c["span_if_claim"] else [],
                "counterpoints": [c["nikil_notes"]] if c["nikil_notes"] else [],
                "gt_spans": gt_spans,
            })

        if not advice_items:
            continue  # skip posts with no qualifying comments

        key = (pid, MODEL_NAME)
        posts[key] = {
            "class_label": info["themes"],
            "post_id": pid,
            "title": info["title"],
            "link": f"https://www.reddit.com/r/suboxone/comments/{pid}/",
            "model_family": "sample",
            "model_name": MODEL_NAME,
            "summary": info["body"],
            "advice": advice_items,
            "divergences": [],
            "clinical_notes": [],
            "data_quality": "",
            "split": split_name,
        }

        comments[pid] = {
            "post_id": pid,
            "title": info["title"],
            "body": info["body"],
            "label1": "",
            "label2": "",
            "label3": "",
            "comments": [c["comment_body"] for c in info["comments"]],
        }

    return posts, comments


def load_all():
    """Load dev and test CSVs. Returns (post_ids, posts, comments, models)."""
    posts = {}
    comments = {}

    for csv_name, split in [("train.csv", "train"), ("dev.csv", "dev"), ("test.csv", "test")]:
        path = BASE / csv_name
        if not path.exists():
            continue
        grouped = _load_csv(path)
        p, c = _build_posts_and_comments(grouped, split)
        for key, val in p.items():
            if key not in posts:
                posts[key] = val
        for pid, cdata in c.items():
            if pid not in comments:
                comments[pid] = cdata

    # Get unique post_ids preserving order
    seen = set()
    post_ids = []
    for (pid, _) in posts:
        if pid not in seen:
            seen.add(pid)
            post_ids.append(pid)

    models = [MODEL_NAME]

    return post_ids, posts, comments, models
