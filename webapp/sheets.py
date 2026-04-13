"""
Google Sheets integration for the MOUD annotation tool.

Provides two-way sync:
  - Push: After each annotation save, write all annotations to an "Annotations" sheet tab
  - Pull: On demand, read input data from the sheet to populate the DB

Requires:
  - GOOGLE_SHEETS_CREDENTIALS_JSON env var (service account JSON as a string)
  - GOOGLE_SHEET_ID env var (the spreadsheet ID from the sheet URL)
"""

import json
import logging
import os
import gspread
from google.oauth2.service_account import Credentials

from models import get_db

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Sheet tab names
ANNOTATIONS_TAB = "Annotations"
CLAIM_SPANS_TAB = "Claim Spans"
EXPERT_REVIEWS_TAB = "Expert Reviews"


def _get_client():
    """Return an authorized gspread client, or None if not configured."""
    creds_json = os.environ.get("GOOGLE_SHEETS_CREDENTIALS_JSON")
    if not creds_json:
        return None
    try:
        creds_info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        return gspread.authorize(creds)
    except Exception as e:
        logger.error("Failed to authorize Google Sheets: %s", e)
        return None


def _get_spreadsheet():
    """Return the gspread Spreadsheet object, or None."""
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    if not sheet_id:
        return None
    client = _get_client()
    if not client:
        return None
    try:
        return client.open_by_key(sheet_id)
    except Exception as e:
        logger.error("Failed to open spreadsheet %s: %s", sheet_id, e)
        return None


def _get_or_create_tab(spreadsheet, tab_name, headers):
    """Get an existing worksheet tab or create it with headers."""
    try:
        ws = spreadsheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=tab_name, rows=1000, cols=len(headers))
        ws.update("A1", [headers])
        ws.format("A1:{}1".format(chr(64 + len(headers))), {"textFormat": {"bold": True}})
    return ws


def is_configured():
    """Check if Google Sheets integration is configured."""
    return bool(os.environ.get("GOOGLE_SHEETS_CREDENTIALS_JSON") and os.environ.get("GOOGLE_SHEET_ID"))


def push_annotations():
    """Push all comment-level codes to the Annotations tab in Google Sheets.

    Replaces the entire sheet content with current DB state.
    """
    spreadsheet = _get_spreadsheet()
    if not spreadsheet:
        return False, "Google Sheets not configured"

    conn = get_db()
    try:
        # ── Comment-level codes ──
        headers = ["post_id", "comment_index", "annotator", "code", "created_at"]
        ws = _get_or_create_tab(spreadsheet, ANNOTATIONS_TAB, headers)

        rows = conn.execute(
            "SELECT post_id, comment_index, annotator_username, code, created_at "
            "FROM comment_codes ORDER BY post_id, comment_index, annotator_username"
        ).fetchall()

        data = [headers]
        for r in rows:
            data.append([r["post_id"], r["comment_index"], r["annotator_username"],
                         r["code"], str(r["created_at"])])

        ws.clear()
        if data:
            ws.update("A1", data)

        # ── Claim spans ──
        span_headers = ["post_id", "comment_index", "annotator", "span_start", "span_end",
                        "span_text", "code", "note", "created_at"]
        ws2 = _get_or_create_tab(spreadsheet, CLAIM_SPANS_TAB, span_headers)

        span_rows = conn.execute(
            "SELECT post_id, comment_index, annotator_username, span_start, span_end, "
            "span_text, code, note, created_at "
            "FROM comment_spans ORDER BY post_id, comment_index, span_start"
        ).fetchall()

        span_data = [span_headers]
        for r in span_rows:
            span_data.append([r["post_id"], r["comment_index"], r["annotator_username"],
                              r["span_start"], r["span_end"], r["span_text"],
                              r["code"], r["note"] or "", str(r["created_at"])])

        ws2.clear()
        if span_data:
            ws2.update("A1", span_data)

        # ── Expert reviews ──
        review_headers = ["post_id", "comment_index", "span_start", "span_end",
                          "span_text", "expert", "verdict", "note", "created_at"]
        ws3 = _get_or_create_tab(spreadsheet, EXPERT_REVIEWS_TAB, review_headers)

        review_rows = conn.execute(
            "SELECT post_id, comment_index, span_start, span_end, span_text, "
            "expert_username, verdict, note, created_at "
            "FROM expert_claim_reviews ORDER BY post_id, comment_index, span_start"
        ).fetchall()

        review_data = [review_headers]
        for r in review_rows:
            review_data.append([r["post_id"], r["comment_index"], r["span_start"],
                                r["span_end"], r["span_text"], r["expert_username"],
                                r["verdict"], r["note"] or "", str(r["created_at"])])

        ws3.clear()
        if review_data:
            ws3.update("A1", review_data)

        conn.close()
        total = len(data) - 1 + len(span_data) - 1 + len(review_data) - 1
        return True, f"Pushed {total} rows across 3 tabs"

    except Exception as e:
        conn.close()
        logger.error("Failed to push annotations: %s", e)
        return False, str(e)


def push_annotations_async():
    """Non-blocking push — logs errors but doesn't block the save response."""
    import threading
    def _push():
        try:
            ok, msg = push_annotations()
            if ok:
                logger.info("Sheets sync: %s", msg)
            else:
                logger.warning("Sheets sync failed: %s", msg)
        except Exception as e:
            logger.error("Sheets async push error: %s", e)
    threading.Thread(target=_push, daemon=True).start()


def pull_input_data():
    """Pull post/comment data from the first sheet tab into the database.

    Expects columns: post_id, title, body, label1, label2, label3, num_comments, reddit_url, comment_1, comment_2, ...
    Only inserts posts that don't already exist in the DB.
    """
    spreadsheet = _get_spreadsheet()
    if not spreadsheet:
        return False, "Google Sheets not configured"

    try:
        # Read from the first tab (assumed to be input data)
        ws = spreadsheet.sheet1
        all_data = ws.get_all_records()
        if not all_data:
            return False, "No data found in sheet"

        conn = get_db()
        inserted_posts = 0
        inserted_comments = 0

        for row in all_data:
            post_id = str(row.get("post_id", "")).strip()
            if not post_id:
                continue

            # Check if post already exists
            existing = conn.execute("SELECT id FROM posts WHERE id = ?", (post_id,)).fetchone()
            if existing:
                continue

            title = str(row.get("title", ""))
            body = str(row.get("body", ""))
            label1 = str(row.get("label1", ""))
            label2 = str(row.get("label2", ""))
            label3 = str(row.get("label3", ""))
            num_comments = int(row.get("num_comments", 0)) if row.get("num_comments") else 0
            reddit_url = str(row.get("reddit_url", ""))

            conn.execute(
                "INSERT INTO posts (id, title, body, label1, label2, label3, num_comments, reddit_url) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (post_id, title, body, label1, label2, label3, num_comments, reddit_url),
            )
            inserted_posts += 1

            # Look for comment columns (comment_1, comment_2, ... or numbered columns after index 7)
            ci = 1
            for key in row:
                if key.lower().startswith("comment_") or key.lower().startswith("comment "):
                    text = str(row[key]).strip()
                    if text and text.lower() != "nan":
                        conn.execute(
                            "INSERT INTO comments (post_id, comment_index, text) VALUES (?, ?, ?)",
                            (post_id, ci, text),
                        )
                        ci += 1
                        inserted_comments += 1

        conn.commit()
        conn.close()
        return True, f"Imported {inserted_posts} new posts, {inserted_comments} comments"

    except Exception as e:
        logger.error("Failed to pull from sheet: %s", e)
        return False, str(e)


def get_sync_status():
    """Return info about the connected sheet for display."""
    spreadsheet = _get_spreadsheet()
    if not spreadsheet:
        return None
    try:
        return {
            "title": spreadsheet.title,
            "url": spreadsheet.url,
            "tabs": [ws.title for ws in spreadsheet.worksheets()],
        }
    except Exception as e:
        logger.error("Failed to get sheet status: %s", e)
        return None
