"""
Reddit scraper using a real browser (Playwright). No API keys needed.

Uses old.reddit.com in a headless browser. If that returns empty, tries new Reddit.

Install:
    pip install playwright pandas openpyxl tqdm
    playwright install chromium

Run:
    python scrape_reddit_posts_browser.py

Debug (saves first page HTML + screenshot, prints what was found):
    DEBUG=1 python scrape_reddit_posts_browser.py

Show browser window:
    SHOW_BROWSER=1 python scrape_reddit_posts_browser.py
"""

import os
import re
import time
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright
from tqdm import tqdm

DEBUG = os.environ.get("DEBUG", "").strip() in ("1", "true", "yes")
SHOW_BROWSER = os.environ.get("SHOW_BROWSER", "").strip() in ("1", "true", "yes")


def link_to_old_reddit_url(link: str) -> str | None:
    """Convert any Reddit post link to old.reddit.com URL."""
    if link is None or not isinstance(link, str) or "reddit.com" not in link:
        return None
    link = link.strip().rstrip("/").replace(".json", "")
    if not link.startswith("http"):
        return None
    link = re.sub(r"^https?://(www\.)?reddit\.com", "https://old.reddit.com", link)
    return link


def link_to_new_reddit_url(link: str) -> str | None:
    """Convert any Reddit post link to www.reddit.com URL."""
    if link is None or not isinstance(link, str) or "reddit.com" not in link:
        return None
    link = link.strip().rstrip("/").replace(".json", "")
    if not link.startswith("http"):
        return None
    link = re.sub(r"^https?://old\.reddit\.com", "https://www.reddit.com", link)
    link = re.sub(r"^https?://(www\.)?reddit\.com", "https://www.reddit.com", link)
    return link


def clean_text(text: str) -> str:
    if not text or not isinstance(text, str):
        return ""
    return " ".join(text.split()).strip()


def _extract_old_reddit(page, debug: bool, script_dir: Path | None) -> tuple[str, str]:
    """Extract from old.reddit.com DOM."""
    post_text = ""
    comments_parts = []
    exp_loc = page.locator("div.expanded div.usertext-body")
    if exp_loc.count() > 0:
        raw = exp_loc.first.inner_text()
        if raw and raw.strip() not in ("[removed]", "[deleted]", ""):
            post_text = clean_text(raw)
    if not post_text:
        for el in page.locator("div.usertext-body").all():
            raw = el.inner_text()
            if raw and len(raw.strip()) > 20 and raw.strip() not in ("[removed]", "[deleted]", ""):
                post_text = clean_text(raw)
                break
    nest_loc = page.locator("div.commentlisting > div.nestedlisting")
    n_nests = nest_loc.count()
    for i in range(n_nests):
        nest = nest_loc.nth(i)
        ut = nest.locator("div.usertext-body").first
        if ut.count() == 0:
            ut = nest.locator("div.md").first
        if ut.count() > 0:
            body = ut.inner_text()
            if body and body.strip() not in ("[removed]", "[deleted]", ""):
                comments_parts.append(f"C{i + 1}: {clean_text(body)}")
    if not comments_parts:
        thing_loc = page.locator("div.commentlisting > div.thing")
        for i in range(thing_loc.count()):
            thing = thing_loc.nth(i)
            ut = thing.locator("div.usertext-body").first
            if ut.count() == 0:
                ut = thing.locator("div.md").first
            if ut.count() > 0:
                body = ut.inner_text()
                if body and body.strip() not in ("[removed]", "[deleted]", ""):
                    comments_parts.append(f"C{i + 1}: {clean_text(body)}")
    if debug and script_dir:
        print(f"    [old] expanded usertext: {exp_loc.count()}, nestedlistings: {n_nests}, comments: {len(comments_parts)}")
    return post_text, "\n".join(comments_parts) if comments_parts else ""


def _extract_new_reddit(page, debug: bool) -> tuple[str, str]:
    """Extract from www.reddit.com (new Reddit) DOM."""
    post_text = ""
    comments_parts = []
    # New Reddit: post body in shreddit-post or [data-testid="post-content"]
    for selector in ('shreddit-post [slot="post-content-container"]', '[data-testid="post-content"]', 'shreddit-post'):
        loc = page.locator(selector)
        if loc.count() > 0:
            raw = loc.first.inner_text()
            if raw and len(raw.strip()) > 10 and raw.strip() not in ("[removed]", "[deleted]", ""):
                post_text = clean_text(raw)
                break
    # Comments: new Reddit uses shreddit-comment or [data-testid="comment"]
    for selector in ('shreddit-comment', '[data-testid="comment"]', 'faceplate-tracker[source="comments"] [data-testid="comment"]'):
        loc = page.locator(selector)
        n = loc.count()
        if n > 0:
            for i in range(min(n, 50)):
                el = loc.nth(i)
                text = el.inner_text()
                if text and len(text.strip()) > 5 and text.strip() not in ("[removed]", "[deleted]", ""):
                    comments_parts.append(f"C{i + 1}: {clean_text(text)}")
            if debug:
                print(f"    [new] selector {selector!r}: {n} elements, kept {len(comments_parts)}")
            break
    return post_text, "\n".join(comments_parts) if comments_parts else ""


def fetch_post_and_comments_with_browser(
    page, url: str, script_dir: Path | None = None, is_debug: bool = False
) -> tuple[str, str]:
    """Load URL and extract post body + top-level comments. Tries old Reddit first, then new."""
    post_text = ""
    top_level_comments_text = ""
    try:
        response = page.goto(url, wait_until="domcontentloaded", timeout=20000)
        if response and response.status >= 400:
            if is_debug:
                print(f"    HTTP {response.status} for {url[:60]}...")
            if is_debug and script_dir:
                time.sleep(0.5)
                (script_dir / "debug_reddit_page.html").write_text(page.content(), encoding="utf-8")
                page.screenshot(path=str(script_dir / "debug_reddit_screenshot.png"))
                print(f"    Saved debug_reddit_page.html and debug_reddit_screenshot.png (403 page) to {script_dir}")
            return "", ""

        page.wait_for_load_state("networkidle", timeout=10000)
        time.sleep(1.5)

        if is_debug and script_dir:
            script_dir.mkdir(parents=True, exist_ok=True)
            (script_dir / "debug_reddit_page.html").write_text(page.content(), encoding="utf-8")
            page.screenshot(path=str(script_dir / "debug_reddit_screenshot.png"))
            print(f"    Saved debug_reddit_page.html and debug_reddit_screenshot.png to {script_dir}")

        is_old = "old.reddit.com" in url
        if is_old:
            post_text, top_level_comments_text = _extract_old_reddit(page, is_debug, script_dir)
        else:
            post_text, top_level_comments_text = _extract_new_reddit(page, is_debug)

        if (not post_text and not top_level_comments_text) and is_old:
            new_url = link_to_new_reddit_url(url.replace("old.reddit.com", "www.reddit.com"))
            if new_url and new_url != url:
                if is_debug:
                    print("    Old Reddit empty, trying new Reddit...")
                response2 = page.goto(new_url, wait_until="domcontentloaded", timeout=20000)
                if response2 and response2.status == 200:
                    time.sleep(2)
                    post_text, top_level_comments_text = _extract_new_reddit(page, is_debug)

    except Exception as e:
        if is_debug:
            print(f"    Error: {e}")
        post_text = ""
        top_level_comments_text = ""

    if is_debug and script_dir:
        print(f"    Result: post_text len={len(post_text)}, comments len={len(top_level_comments_text)}")
    return post_text, top_level_comments_text


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    input_candidates = [
        script_dir / "Top10_High_Only_Per_Class.xlsx",
        script_dir / "Top10_High_With_Text_And_Comments.xlsx",
    ]
    input_path = next((p for p in input_candidates if p.exists()), None)
    if input_path is None:
        raise SystemExit(
            "Input file not found. Put one of these in the same folder as this script:\n"
            "  Top10_High_Only_Per_Class.xlsx\n  Top10_High_With_Text_And_Comments.xlsx"
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
    if SHOW_BROWSER:
        print("Opening visible browser window...")
    else:
        print("Opening browser (Chromium)...")
    if DEBUG:
        print("DEBUG mode: only first row, will save HTML + screenshot.")

    post_texts = []
    comments_texts = []
    rows = list(df.iterrows())
    if DEBUG and rows:
        rows = rows[:1]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not SHOW_BROWSER)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        )
        page = context.new_page()

        for idx, (_, row) in enumerate(tqdm(rows, total=len(rows), desc="Scraping")):
            link = row.get(link_col)
            url = link_to_old_reddit_url(link)
            if not url:
                post_texts.append("")
                comments_texts.append("")
                time.sleep(0.5)
                continue
            is_debug = DEBUG and idx == 0
            post_text, comments_text = fetch_post_and_comments_with_browser(
                page, url, script_dir=script_dir if is_debug else None, is_debug=is_debug
            )
            post_texts.append(post_text)
            comments_texts.append(comments_text)
            time.sleep(0.5)

        browser.close()

    if DEBUG and len(rows) < len(df):
        post_texts.extend([""] * (len(df) - len(post_texts)))
        comments_texts.extend([""] * (len(df) - len(comments_texts)))

    df["post_text"] = post_texts
    df["top_level_comments_text"] = comments_texts
    out_path = script_dir / "Top10_High_With_Text_And_Comments.xlsx"
    df.to_excel(out_path, index=False, engine="openpyxl")
    print(f"Saved to {out_path}")


if __name__ == "__main__":
    main()
