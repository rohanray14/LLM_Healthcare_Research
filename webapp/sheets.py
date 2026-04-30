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
import threading
import gspread
from google.oauth2.service_account import Credentials

from models import get_db, get_user_setting, set_user_setting

logger = logging.getLogger(__name__)

# Serialize all push operations so concurrent saves don't interleave
_push_lock = threading.Lock()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Sheet tab names
ANNOTATIONS_TAB = "Annotations"
CLAIM_SPANS_TAB = "Claim Spans"
EXPERT_REVIEWS_TAB = "Expert Reviews"
ANNOTATOR_PROGRESS_TAB = "Annotator Progress"


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


def get_active_sheet_id(username=None):
    """Return the active sheet ID for a user (DB-persisted override, then env var)."""
    if username:
        override = get_user_setting(username, "sheet_id")
        if override:
            return override
    return os.environ.get("GOOGLE_SHEET_ID")


def set_sheet_id(sheet_id, username=None):
    """Persist a per-user override for the Google Sheet ID.

    Accepts either a raw sheet ID or a full Google Sheets URL.
    Survives app restarts and works across gunicorn workers.
    """
    if not sheet_id:
        if username:
            set_user_setting(username, "sheet_id", "")
        return
    sheet_id = sheet_id.strip()
    # Extract ID from full URL: docs.google.com/spreadsheets/d/SHEET_ID/...
    if "/spreadsheets/d/" in sheet_id:
        sheet_id = sheet_id.split("/spreadsheets/d/")[1].split("/")[0]
    if username:
        set_user_setting(username, "sheet_id", sheet_id)


def _get_spreadsheet(username=None):
    """Return the gspread Spreadsheet object, or None."""
    sheet_id = get_active_sheet_id(username)
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


def is_configured(username=None):
    """Check if Google Sheets integration is configured."""
    return bool(os.environ.get("GOOGLE_SHEETS_CREDENTIALS_JSON") and get_active_sheet_id(username))


def _safe_write_tab(ws, data):
    """Write data to a sheet tab safely: write first, then trim leftover rows.

    This avoids the clear-then-write pattern that can leave sheets empty on failure.
    """
    # Write all new data starting from A1
    if data:
        ws.update("A1", data)

    # Now clear any leftover rows below the new data
    total_rows_in_sheet = ws.row_count
    new_row_count = len(data)
    if total_rows_in_sheet > new_row_count and new_row_count > 0:
        # Build a range for the leftover rows and clear them
        last_col = chr(64 + len(data[0]))  # e.g. 'E' for 5 columns
        leftover_range = f"A{new_row_count + 1}:{last_col}{total_rows_in_sheet}"
        ws.batch_clear([leftover_range])


def push_annotations(username=None):
    """Push all comment-level codes to the Annotations tab in Google Sheets.

    Replaces the entire sheet content with current DB state.
    Uses a lock to prevent concurrent pushes from interleaving, and writes
    data before clearing leftovers to prevent data loss on failure.
    """
    with _push_lock:
        spreadsheet = _get_spreadsheet(username)
        if not spreadsheet:
            return False, "Google Sheets not configured"

        conn = get_db()
        try:
            # ── Collect all data from DB first (single snapshot) ──

            # Comment-level codes
            headers = ["post_id", "comment_index", "annotator", "code", "created_at"]
            rows = conn.execute(
                "SELECT post_id, comment_index, annotator_username, code, created_at "
                "FROM comment_codes ORDER BY post_id, comment_index, annotator_username"
            ).fetchall()
            data = [headers]
            for r in rows:
                data.append([r["post_id"], r["comment_index"], r["annotator_username"],
                             r["code"], str(r["created_at"])])

            # Claim spans
            span_headers = ["post_id", "comment_index", "annotator", "span_start", "span_end",
                            "span_text", "code", "note", "created_at"]
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

            # Expert reviews
            review_headers = ["post_id", "comment_index", "span_start", "span_end",
                              "span_text", "expert", "verdict", "note", "created_at"]
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

            # ── Write to sheets (write-then-trim, not clear-then-write) ──
            ws = _get_or_create_tab(spreadsheet, ANNOTATIONS_TAB, headers)
            _safe_write_tab(ws, data)

            ws2 = _get_or_create_tab(spreadsheet, CLAIM_SPANS_TAB, span_headers)
            _safe_write_tab(ws2, span_data)

            ws3 = _get_or_create_tab(spreadsheet, EXPERT_REVIEWS_TAB, review_headers)
            _safe_write_tab(ws3, review_data)

            # Annotator progress summary (queries DB, so must be before conn.close)
            progress_headers = ["annotator", "posts_annotated", "total_codes", "total_spans", "last_activity"]
            progress_rows = conn.execute(
                "SELECT annotator_username, "
                "COUNT(DISTINCT post_id) AS posts_annotated, "
                "COUNT(*) AS total_codes, "
                "MAX(created_at) AS last_activity "
                "FROM comment_codes GROUP BY annotator_username "
                "ORDER BY annotator_username"
            ).fetchall()
            span_counts = dict(conn.execute(
                "SELECT annotator_username, COUNT(*) "
                "FROM comment_spans GROUP BY annotator_username"
            ).fetchall())
            progress_data = [progress_headers]
            for r in progress_rows:
                progress_data.append([
                    r["annotator_username"],
                    r["posts_annotated"],
                    r["total_codes"],
                    span_counts.get(r["annotator_username"], 0),
                    str(r["last_activity"]),
                ])

            conn.close()

            ws4 = _get_or_create_tab(spreadsheet, ANNOTATOR_PROGRESS_TAB, progress_headers)
            _safe_write_tab(ws4, progress_data)

            total = len(data) - 1 + len(span_data) - 1 + len(review_data) - 1
            return True, f"Pushed {total} rows across 4 tabs"

        except Exception as e:
            conn.close()
            logger.error("Failed to push annotations: %s", e)
            return False, str(e)


def push_annotations_async(username=None):
    """Non-blocking push — logs errors but doesn't block the save response.

    Uses the _push_lock internally (via push_annotations) so concurrent
    saves are serialized. If a push is already in progress, the new one
    will simply queue behind it and get the latest DB state when it runs.
    """
    def _push():
        try:
            ok, msg = push_annotations(username)
            if ok:
                logger.info("Sheets sync: %s", msg)
            else:
                logger.warning("Sheets sync failed: %s", msg)
        except Exception as e:
            logger.error("Sheets async push error: %s", e)
    threading.Thread(target=_push, daemon=True).start()


def pull_input_data(username=None, tab_name=None):
    """Pull post/comment data from a sheet tab into the database.

    Expects columns: post_id, title, body, label1, label2, label3, num_comments, reddit_url, comment_1, comment_2, ...
    Only inserts posts that don't already exist in the DB.
    """
    spreadsheet = _get_spreadsheet(username)
    if not spreadsheet:
        return False, "Google Sheets not configured"

    try:
        # Read from the specified tab, or fall back to the first tab
        if tab_name:
            try:
                ws = spreadsheet.worksheet(tab_name)
            except gspread.exceptions.WorksheetNotFound:
                return False, f"Tab '{tab_name}' not found in spreadsheet"
        else:
            ws = spreadsheet.sheet1
        all_data = ws.get_all_records()
        if not all_data:
            return False, "No data found in sheet"

        # Validate required columns exist
        required_cols = {"post_id", "title", "body"}
        actual_cols = {str(k).strip().lower() for k in all_data[0].keys()}
        # Build a case-insensitive mapping from actual column names
        col_map = {str(k).strip().lower(): k for k in all_data[0].keys()}
        missing = required_cols - actual_cols
        if missing:
            return False, (
                f"Missing required columns: {', '.join(sorted(missing))}. "
                f"Found columns: {', '.join(sorted(col_map.values()))}. "
                f"Expected at minimum: post_id, title, body"
            )

        conn = get_db()
        inserted_posts = 0
        inserted_comments = 0

        def _get(row, name, default=""):
            """Case-insensitive column lookup."""
            # Try exact match first, then case-insensitive
            if name in row:
                return row[name]
            for k in row:
                if str(k).strip().lower() == name.lower():
                    return row[k]
            return default

        for row in all_data:
            post_id = str(_get(row, "post_id", "")).strip()
            if not post_id:
                continue

            # Check if post already exists
            existing = conn.execute("SELECT id FROM posts WHERE id = ?", (post_id,)).fetchone()
            if existing:
                continue

            title = str(_get(row, "title"))
            body = str(_get(row, "body"))
            label1 = str(_get(row, "label1"))
            label2 = str(_get(row, "label2"))
            label3 = str(_get(row, "label3"))
            nc = _get(row, "num_comments", 0)
            num_comments = int(nc) if nc else 0
            reddit_url = str(_get(row, "reddit_url"))

            conn.execute(
                "INSERT INTO posts (id, title, body, label1, label2, label3, num_comments, reddit_url) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (post_id, title, body, label1, label2, label3, num_comments, reddit_url),
            )
            inserted_posts += 1

            # Look for comment columns (comment_1, comment_2, ... or numbered columns after index 7)
            ci = 1
            for key in row:
                if str(key).lower().startswith("comment_") or str(key).lower().startswith("comment "):
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


def get_sync_status(username=None):
    """Return info about the connected sheet for display."""
    spreadsheet = _get_spreadsheet(username)
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
