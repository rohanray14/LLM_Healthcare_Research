"""
Select top N posts per class from the 6K dataset, ranked by Reddit upvotes (score).

Since the 6K dataset doesn't include upvote counts, this script:
1. Scrapes upvote scores from Reddit via PRAW for all posts
2. Selects top N per class by score
3. Outputs an Excel file ready for run_full_pipeline.py

Requires: REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT in .env

Usage:
  python prepare_top_n_by_upvotes.py              # default: top 30
  python prepare_top_n_by_upvotes.py --top 20     # top 20 per class
  python prepare_top_n_by_upvotes.py --top 30     # top 30 per class
  python prepare_top_n_by_upvotes.py --cache-only # use cached scores, don't scrape
"""

import argparse
import os
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

SCRIPT_DIR = Path(__file__).resolve().parent
FULL_DATA = SCRIPT_DIR / "6K_data_with_comments (1).xlsx"
SCORE_CACHE = SCRIPT_DIR / "reddit_scores_cache.xlsx"


def combine_comments(row):
    parts = []
    for i in range(1, 11):
        col = f"Comment{i}"
        val = row.get(col)
        if pd.notna(val) and str(val).strip():
            parts.append(f"C{i}: {str(val).strip()}")
    return "\n\n".join(parts)


def scrape_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Scrape upvote scores from Reddit for all post IDs."""
    for var in ("REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET", "REDDIT_USER_AGENT"):
        if not os.environ.get(var):
            raise SystemExit(
                f"Missing {var}. Set Reddit API credentials in .env.\n"
                "Create app at https://www.reddit.com/prefs/apps (type: script)"
            )

    reddit = praw.Reddit(
        client_id=os.environ["REDDIT_CLIENT_ID"],
        client_secret=os.environ["REDDIT_CLIENT_SECRET"],
        user_agent=os.environ["REDDIT_USER_AGENT"],
    )

    # Load existing cache to avoid re-scraping
    cached = {}
    if SCORE_CACHE.exists():
        cache_df = pd.read_excel(SCORE_CACHE)
        cached = dict(zip(cache_df["post_id"].astype(str), cache_df["score"]))
        print(f"Loaded {len(cached)} cached scores")

    scores = []
    post_ids = df["id"].astype(str).tolist()

    for post_id in tqdm(post_ids, desc="Scraping upvotes"):
        if post_id in cached:
            scores.append(cached[post_id])
            continue
        try:
            submission = reddit.submission(id=post_id)
            score = submission.score
            scores.append(score)
            cached[post_id] = score
        except Exception as e:
            scores.append(0)
        time.sleep(0.5)

    df = df.copy()
    df["score"] = scores

    # Save cache
    cache_df = pd.DataFrame({"post_id": list(cached.keys()), "score": list(cached.values())})
    cache_df.to_excel(SCORE_CACHE, index=False)
    print(f"Cached {len(cached)} scores to {SCORE_CACHE.name}")

    return df


def main():
    parser = argparse.ArgumentParser(description="Select top N posts per class by upvotes")
    parser.add_argument("--top", type=int, default=30, help="Number of top posts per class (default: 30)")
    parser.add_argument("--cache-only", action="store_true", help="Use cached scores only, don't scrape")
    args = parser.parse_args()
    top_n = args.top

    df_full = pd.read_excel(FULL_DATA)
    df_full["class_label"] = df_full["Label1"].replace(
        "Psychophysical Effects", "Psycho-Physical Effects"
    )
    df_full["top_level_comments_text"] = df_full.apply(combine_comments, axis=1)

    if args.cache_only:
        if not SCORE_CACHE.exists():
            raise SystemExit(f"No cache found at {SCORE_CACHE.name}. Run without --cache-only first.")
        cache_df = pd.read_excel(SCORE_CACHE)
        score_map = dict(zip(cache_df["post_id"].astype(str), cache_df["score"]))
        df_full["score"] = df_full["id"].astype(str).map(score_map).fillna(0).astype(int)
        print(f"Loaded {len(score_map)} cached scores")
    else:
        df_full = scrape_scores(df_full)

    frames = []
    for cls in sorted(df_full["class_label"].unique()):
        sub = df_full[df_full["class_label"] == cls].sort_values("score", ascending=False)
        batch = sub.head(top_n).copy()
        batch["rank_in_group"] = range(1, 1 + len(batch))
        batch["engagement_level"] = "High"
        frames.append(batch)
        print(f"{cls}: {len(batch)} posts (upvotes range: {batch['score'].max()}-{batch['score'].min()})")

    result = pd.concat(frames, ignore_index=True)
    result = result.rename(columns={
        "id": "post_id",
        "body": "post_text",
    })
    result["link"] = "https://www.reddit.com/r/suboxone/comments/" + result["post_id"] + "/"

    output_cols = [
        "class_label", "engagement_level", "rank_in_group",
        "post_id", "title", "score", "number_top_level_comment", "link",
        "post_text", "top_level_comments_text",
    ]
    result = result[output_cols].rename(columns={
        "number_top_level_comment": "top_level_comment_count",
        "score": "upvote_score",
    })

    output_path = SCRIPT_DIR / f"Top{top_n}_By_Upvotes_Input.xlsx"
    result.to_excel(output_path, index=False)
    print(f"\nSaved {len(result)} posts to {output_path.name}")
    print(f"Classes: {result['class_label'].value_counts().to_dict()}")


if __name__ == "__main__":
    main()
