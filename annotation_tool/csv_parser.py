import csv, re, io, json
from collections import OrderedDict

# Color palette for auto-assigning code styles
CODE_PALETTE = [
    {"color": "#fee2e2", "border_color": "#ef4444"},  # red
    {"color": "#dbeafe", "border_color": "#3b82f6"},  # blue
    {"color": "#fef9c3", "border_color": "#eab308"},  # yellow
    {"color": "#dcfce7", "border_color": "#22c55e"},  # green
    {"color": "#e0e7ff", "border_color": "#6366f1"},  # indigo
    {"color": "#f3e8ff", "border_color": "#a855f7"},  # purple
    {"color": "#ffe4e6", "border_color": "#f43f5e"},  # rose
    {"color": "#ccfbf1", "border_color": "#14b8a6"},  # teal
]

SPAN_PALETTE = [
    {"color": "#fee2e2", "border_color": "#ef4444"},  # red
    {"color": "#fef9c3", "border_color": "#eab308"},  # yellow
    {"color": "#dbeafe", "border_color": "#3b82f6"},  # blue
    {"color": "#dcfce7", "border_color": "#22c55e"},  # green
]


def _norm(text):
    return re.sub(r'\s+', ' ', text).strip().strip('"').strip("'")


def _find_span_offsets(comment_text, span_text):
    idx = comment_text.find(span_text)
    if idx >= 0:
        return idx, idx + len(span_text)

    stripped = span_text.strip('"').strip("'").strip()
    if stripped != span_text:
        idx = comment_text.find(stripped)
        if idx >= 0:
            return idx, idx + len(stripped)

    norm_comment = _norm(comment_text)
    norm_span = _norm(span_text)
    idx = norm_comment.find(norm_span)
    if idx >= 0:
        return _map_norm_offset(comment_text, norm_comment, idx, len(norm_span))

    idx = norm_comment.lower().find(norm_span.lower())
    if idx >= 0:
        return _map_norm_offset(comment_text, norm_comment, idx, len(norm_span))

    for prefix_len in [60, 40, 25, 15]:
        if len(norm_span) < prefix_len:
            continue
        prefix = norm_span[:prefix_len]
        idx = norm_comment.find(prefix)
        if idx < 0:
            idx = norm_comment.lower().find(prefix.lower())
        if idx >= 0:
            end_idx = min(idx + len(norm_span), len(norm_comment))
            return _map_norm_offset(comment_text, norm_comment, idx, end_idx - idx)

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
    orig_i = 0
    norm_i = 0
    while orig_i < len(original) and norm_i < norm_start:
        if original[orig_i].isspace():
            if orig_i == 0 or not original[orig_i - 1].isspace():
                norm_i += 1
            orig_i += 1
        else:
            norm_i += 1
            orig_i += 1
    start_orig = orig_i

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


def _detect_columns(headers):
    """Detect which CSV columns map to which fields."""
    mapping = {}
    h_lower = {h.lower().strip(): h for h in headers}

    # Post ID
    for candidate in ["postid", "post_id", "postId"]:
        if candidate.lower() in h_lower:
            mapping["post_id"] = h_lower[candidate.lower()]
            break

    # Title
    for candidate in ["title"]:
        if candidate in h_lower:
            mapping["title"] = h_lower[candidate]
            break

    # Body
    for candidate in ["body", "post_body", "selftext"]:
        if candidate in h_lower:
            mapping["body"] = h_lower[candidate]
            break

    # Comment
    for candidate in ["comment", "comment_body", "commentbody"]:
        if candidate in h_lower:
            mapping["comment"] = h_lower[candidate]
            break

    # Comment ID
    for candidate in ["comment_id", "commentid"]:
        if candidate in h_lower:
            mapping["comment_id"] = h_lower[candidate]
            break

    # Labels
    for candidate in ["labels", "label", "topic", "flair"]:
        if candidate in h_lower:
            mapping["labels"] = h_lower[candidate]
            break

    # L1 coding
    for candidate in ["l1_coding", "l1coding", "codes", "coding"]:
        if candidate in h_lower:
            mapping["l1_coding"] = h_lower[candidate]
            break

    # Split
    for candidate in ["split", "dataset", "set"]:
        if candidate in h_lower:
            mapping["split"] = h_lower[candidate]
            break

    # Link
    for candidate in ["link", "url", "permalink"]:
        if candidate in h_lower:
            mapping["link"] = h_lower[candidate]
            break

    # Span columns: any column starting with "span_if_"
    span_columns = {}
    for h in headers:
        if h.lower().startswith("span_if_"):
            span_type = h[len("span_if_"):].upper()
            span_columns[span_type] = h

    mapping["span_columns"] = span_columns
    return mapping


def parse_csv_content(content, link_template=""):
    """Parse CSV content string and return (posts_data, config).

    posts_data: OrderedDict of {post_id: {title, body, labels, split, link, comments: [...], pre_annotations: {...}}}
    config: {comment_codes: {...}, span_types: [...]}
    """
    reader = csv.DictReader(io.StringIO(content))
    headers = reader.fieldnames or []
    mapping = _detect_columns(headers)

    if "post_id" not in mapping:
        raise ValueError("CSV must have a 'postId' or 'post_id' column")
    if "comment" not in mapping:
        raise ValueError("CSV must have a 'comment' or 'comment_body' column")

    # Group rows by post
    grouped = OrderedDict()
    all_codes = set()

    for row in reader:
        pid = (row.get(mapping["post_id"]) or "").strip()
        if not pid:
            continue

        if pid not in grouped:
            grouped[pid] = {
                "title": (row.get(mapping.get("title", ""), "") or "").strip(),
                "body": (row.get(mapping.get("body", ""), "") or "").strip(),
                "labels": (row.get(mapping.get("labels", ""), "") or "").strip(),
                "split": (row.get(mapping.get("split", ""), "") or "").strip(),
                "link": (row.get(mapping.get("link", ""), "") or "").strip(),
                "comments": [],
            }

        comment_text = (row.get(mapping["comment"]) or "").strip()
        comment_id = (row.get(mapping.get("comment_id", ""), "") or "").strip()
        l1_coding = (row.get(mapping.get("l1_coding", ""), "") or "").strip()

        # Collect span texts per span type
        span_texts = {}
        for span_type, col_name in mapping.get("span_columns", {}).items():
            val = (row.get(col_name) or "").strip()
            if val:
                span_texts[span_type] = val

        # Track all unique codes
        if l1_coding:
            for code in l1_coding.split(","):
                code = code.strip()
                if code:
                    all_codes.add(code)

        grouped[pid]["comments"].append({
            "comment_id": comment_id,
            "comment_body": comment_text,
            "l1_coding": l1_coding,
            "span_texts": span_texts,
        })

    # Build posts_data with pre-annotations
    posts_data = OrderedDict()
    span_types = sorted(mapping.get("span_columns", {}).keys())

    for pid, info in grouped.items():
        if not info["comments"]:
            continue

        pre_annotations = {}
        comments_clean = []

        for i, c in enumerate(info["comments"]):
            comments_clean.append({
                "comment_id": c["comment_id"],
                "comment_body": c["comment_body"],
            })

            # Parse codes
            codes = []
            if c["l1_coding"]:
                codes = [code.strip() for code in c["l1_coding"].split(",") if code.strip()]

            # Parse spans
            spans = []
            for span_type, span_text in c.get("span_texts", {}).items():
                span_parts = [s.strip() for s in span_text.split("\n") if s.strip()]
                seen_offsets = set()
                for part in span_parts:
                    start, end = _find_span_offsets(c["comment_body"], part)
                    if start is not None and (start, end) not in seen_offsets:
                        seen_offsets.add((start, end))
                        spans.append({
                            "start": start,
                            "end": end,
                            "text": c["comment_body"][start:end],
                            "span_type": span_type,
                        })

            if codes or spans:
                pre_annotations[i] = {"codes": codes, "spans": spans}

        # Generate link
        link = info.get("link", "")
        if not link and link_template and pid:
            link = link_template.replace("{post_id}", pid)

        posts_data[pid] = {
            "title": info["title"],
            "body": info["body"],
            "labels": info["labels"],
            "split": info["split"],
            "link": link,
            "comments": comments_clean,
            "pre_annotations": pre_annotations,
        }

    # Build config
    config = _build_config(all_codes, span_types)
    return posts_data, config


def _build_config(all_codes, span_types):
    """Build comment_codes config from detected codes and span types."""
    comment_codes = {}
    color_idx = 0

    # Add span type codes first
    for i, st in enumerate(span_types):
        palette = SPAN_PALETTE[i % len(SPAN_PALETTE)]
        comment_codes[st] = {
            "label": st,
            "description": f"Auto-added when you highlight a {st} span.",
            "color": palette["color"],
            "border_color": palette["border_color"],
            "is_span_code": True,
        }

    # Add detected codes (excluding span type codes)
    non_span_codes = sorted(all_codes - set(span_types) - {"EXCLUDE"})
    for code in non_span_codes:
        palette = CODE_PALETTE[color_idx % len(CODE_PALETTE)]
        color_idx += 1
        comment_codes[code] = {
            "label": code,
            "description": f"{code} code from dataset.",
            "color": palette["color"],
            "border_color": palette["border_color"],
            "is_span_code": False,
        }

    # Always add EXCLUDE
    comment_codes["EXCLUDE"] = {
        "label": "Exclude",
        "description": "Comment content is irrelevant and should be excluded from analysis.",
        "color": "#f1f5f9",
        "border_color": "#64748b",
        "is_span_code": False,
        "needs_reason": True,
    }

    return {
        "comment_codes": comment_codes,
        "span_types": span_types,
    }
