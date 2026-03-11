"""
Remove the placeholder/filler text from post_text and top_level_comments_text
so the columns are blank again, then you can re-run the scraper.

Run: python clear_placeholder_and_rerun.py
"""

from pathlib import Path

import pandas as pd

def main() -> None:
    script_dir = Path(__file__).resolve().parent
    path = script_dir / "Top10_High_With_Text_And_Comments.xlsx"
    if not path.exists():
        print(f"File not found: {path}")
        return
    df = pd.read_excel(path, engine="openpyxl")
    changed = 0
    if "post_text" in df.columns:
        mask = df["post_text"].astype(str).str.contains("This is a placeholder for the actual post text", na=False)
        if mask.any():
            df.loc[mask, "post_text"] = ""
            changed += int(mask.sum())
    if "top_level_comments_text" in df.columns:
        mask = df["top_level_comments_text"].astype(str).str.contains(
            "This is a placeholder for the actual top-level comments", na=False
        )
        if mask.any():
            df.loc[mask, "top_level_comments_text"] = ""
            changed += int(mask.sum())
    df.to_excel(path, index=False, engine="openpyxl")
    print(f"Cleared placeholder text from {path.name} ({changed} cells updated).")
    print("Re-run the scraper: DEBUG=1 python scrape_reddit_posts_browser.py")


if __name__ == "__main__":
    main()
