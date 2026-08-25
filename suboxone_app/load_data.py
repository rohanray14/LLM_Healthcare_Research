import csv, re
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


def _norm(text):
    """Normalize whitespace and strip quotes for comparison."""
    return re.sub(r'\s+', ' ', text).strip().strip('"').strip("'")


def _find_span_offsets(comment_text, span_text):
    """Find start/end offsets of span_text within comment_text.
    Uses multiple strategies: exact, whitespace-normalized, quote-stripped,
    prefix-based expansion, and longest-prefix matching."""

    # Strategy 1: Exact substring match
    idx = comment_text.find(span_text)
    if idx >= 0:
        return idx, idx + len(span_text)

    # Strategy 2: Quote-stripped match
    stripped = span_text.strip('"').strip("'").strip()
    if stripped != span_text:
        idx = comment_text.find(stripped)
        if idx >= 0:
            return idx, idx + len(stripped)

    # Strategy 3: Whitespace-normalized match
    norm_comment = _norm(comment_text)
    norm_span = _norm(span_text)
    idx = norm_comment.find(norm_span)
    if idx >= 0:
        # Map normalized offset back to original text
        return _map_norm_offset(comment_text, norm_comment, idx, len(norm_span))

    # Strategy 4: Case-insensitive normalized match
    idx = norm_comment.lower().find(norm_span.lower())
    if idx >= 0:
        return _map_norm_offset(comment_text, norm_comment, idx, len(norm_span))

    # Strategy 5: Prefix-based — find where the span starts in the comment,
    # then extend to cover as much of the comment as the span is long.
    # Use progressively shorter prefixes to find anchor point.
    for prefix_len in [60, 40, 25, 15]:
        if len(norm_span) < prefix_len:
            continue
        prefix = norm_span[:prefix_len]
        idx = norm_comment.find(prefix)
        if idx < 0:
            idx = norm_comment.lower().find(prefix.lower())
        if idx >= 0:
            # Found anchor — extend to span length, but don't exceed comment
            end_idx = min(idx + len(norm_span), len(norm_comment))
            return _map_norm_offset(comment_text, norm_comment, idx, end_idx - idx)

    # Strategy 6: Multi-fragment — span has comma/period-separated claims that
    # appear at different positions in the comment. Find each fragment.
    # Return the span covering from first match to last match.
    fragments = [f.strip() for f in re.split(r'[,.]', norm_span) if len(f.strip()) > 10]
    if len(fragments) >= 2:
        positions = []
        for frag in fragments:
            fi = norm_comment.find(frag)
            if fi < 0:
                fi = norm_comment.lower().find(frag.lower())
            if fi >= 0:
                positions.append((fi, fi + len(frag)))
        if len(positions) >= len(fragments) // 2:
            first = min(p[0] for p in positions)
            last = max(p[1] for p in positions)
            return _map_norm_offset(comment_text, norm_comment, first, last - first)

    return None, None


def _map_norm_offset(original, normalized, norm_start, norm_len):
    """Map an offset in whitespace-normalized text back to the original text."""
    # Walk through original text, tracking position in normalized version
    orig_i = 0
    norm_i = 0
    start_orig = None
    end_orig = None

    # Skip leading whitespace in normalized
    while orig_i < len(original) and norm_i < norm_start:
        if original[orig_i].isspace():
            # In normalized, consecutive whitespace = 1 space
            if orig_i == 0 or not original[orig_i - 1].isspace():
                norm_i += 1
            orig_i += 1
        else:
            norm_i += 1
            orig_i += 1

    start_orig = orig_i

    # Now walk norm_len characters
    chars_counted = 0
    while orig_i < len(original) and chars_counted < norm_len:
        if original[orig_i].isspace():
            if orig_i == 0 or not original[orig_i - 1].isspace():
                chars_counted += 1
            orig_i += 1
        else:
            chars_counted += 1
            orig_i += 1

    end_orig = orig_i
    if start_orig is not None and end_orig is not None and end_orig > start_orig:
        return start_orig, end_orig
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
                    seen_offsets = set()
                    for span_text in span_parts:
                        start, end = _find_span_offsets(c["comment_body"], span_text)
                        if start is not None and (start, end) not in seen_offsets:
                            seen_offsets.add((start, end))
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
