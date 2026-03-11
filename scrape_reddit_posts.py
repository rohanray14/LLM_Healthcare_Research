"""
Reddit post and top-level comments scraper.

Install dependencies:
    pip install praw pandas openpyxl tqdm

Set environment variables before running:
    REDDIT_CLIENT_ID
    REDDIT_CLIENT_SECRET
    REDDIT_USER_AGENT
"""

import os
import re
import time
from pathlib import Path

import pandas as pd
import praw
from tqdm import tqdm

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass


def get_post_id_from_link(link: str) -> str | None:
    """Extract Reddit post ID from post URL."""
    if pd.isna(link) or not isinstance(link, str):
        return None
    match = re.search(r"/comments/([a-z0-9]+)", link, re.IGNORECASE)
    return match.group(1) if match else None


def clean_text(text: str) -> str:
    """Normalize whitespace and strip."""
    if not text or not isinstance(text, str):
        return ""
    return " ".join(text.split()).strip()


def fetch_post_and_comments(reddit: praw.Reddit, post_id: str) -> tuple[str, str]:
    """
    Fetch submission selftext and top-level comments only.
    Returns (post_text, top_level_comments_text).
    """
    post_text = ""
    comments_parts = []

    try:
        submission = reddit.submission(id=post_id)

        if submission.selftext is not None:
            raw = submission.selftext.strip()
            if raw and raw not in ("[removed]", "[deleted]"):
                post_text = clean_text(raw)

        submission.comment_sort = "best"
        submission.comments.replace_more(limit=0)

        for i, comment in enumerate(submission.comments, start=1):
            if getattr(comment, "body", None) is None:
                continue
            body = comment.body.strip()
            if body in ("[removed]", "[deleted]", ""):
                continue
            comments_parts.append(f"C{i}: {clean_text(body)}")

        top_level_comments_text = "\n".join(comments_parts) if comments_parts else ""

    except Exception:
        post_text = ""
        top_level_comments_text = ""

    return post_text, top_level_comments_text


def main() -> None:
    required_vars = ("REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET", "REDDIT_USER_AGENT")
    missing = [v for v in required_vars if not os.environ.get(v)]
    if missing:
        raise SystemExit(
            f"Missing environment variables: {', '.join(missing)}\n"
            "Create a Reddit app at https://www.reddit.com/prefs/apps (type: script), "
            "then set REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT."
        )

    reddit = praw.Reddit(
        client_id=os.environ["REDDIT_CLIENT_ID"],
        client_secret=os.environ["REDDIT_CLIENT_SECRET"],
        user_agent=os.environ["REDDIT_USER_AGENT"],
    )

    script_dir = Path(__file__).resolve().parent
    input_candidates = [
        script_dir / "Top10_High_Only_Per_Class.xlsx",
        script_dir / "Top10_High_With_Text_And_Comments.xlsx",
    ]
    input_path = None
    for p in input_candidates:
        if p.exists():
            input_path = p
            break
    if input_path is None:
        raise SystemExit(
            f"Input file not found. Put one of these in the same folder as this script:\n"
            f"  {input_candidates[0].name}\n  {input_candidates[1].name}"
        )

    print(f"Reading: {input_path.name}")
    df = pd.read_excel(input_path, engine="openpyxl")
    for candidate in ("link", "top_level_c_link", "top_level_link"):
        if candidate in df.columns:
            link_col = candidate
            break
    else:
        raise SystemExit(
            "Excel must have a column named 'link', 'top_level_c_link', or 'top_level_link'."
        )
    print(f"Using link column: {link_col}")

    post_texts = []
    comments_texts = []

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Scraping"):
        link = row.get(link_col)
        post_id = get_post_id_from_link(link)

        if not post_id:
            post_texts.append("")
            comments_texts.append("")
            time.sleep(0.5)
            continue

        post_text, comments_text = fetch_post_and_comments(reddit, post_id)
        post_texts.append(post_text)
        comments_texts.append(comments_text)
        time.sleep(0.5)

    df["post_text"] = post_texts
    df["top_level_comments_text"] = comments_texts

    out_path = script_dir / "Top10_High_With_Text_And_Comments.xlsx"
    df.to_excel(out_path, index=False, engine="openpyxl")
    print(f"Saved to {out_path}")


if __name__ == "__main__":
    main()
