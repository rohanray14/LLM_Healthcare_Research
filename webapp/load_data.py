"""Import Excel data into SQLite. Run once before starting the app."""
import os
import sys
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from models import init_db, get_db
from config import DATA_DIR


def load_6k_data():
    path = os.path.join(DATA_DIR, "6K_data_with_comments (1).xlsx")
    print(f"Loading {path} ...")
    df = pd.read_excel(path, engine="openpyxl")

    conn = get_db()
    post_count = 0
    comment_count = 0

    def normalize_label(val):
        if val and val.strip() == "Psycho-Physical Effects":
            return "Psychophysical Effects"
        return val

    for _, row in df.iterrows():
        post_id = str(row.iloc[0])
        title = str(row.iloc[1]) if pd.notna(row.iloc[1]) else ""
        body = str(row.iloc[2]) if pd.notna(row.iloc[2]) else ""
        label1 = normalize_label(str(row.iloc[3])) if pd.notna(row.iloc[3]) else None
        label2 = normalize_label(str(row.iloc[4])) if pd.notna(row.iloc[4]) else None
        label3 = normalize_label(str(row.iloc[5])) if pd.notna(row.iloc[5]) else None
        num_comments = int(row.iloc[6]) if pd.notna(row.iloc[6]) else 0
        reddit_url = f"https://www.reddit.com/r/suboxone/comments/{post_id}/"

        conn.execute(
            "INSERT OR IGNORE INTO posts (id, title, body, label1, label2, label3, num_comments, reddit_url) VALUES (?,?,?,?,?,?,?,?)",
            (post_id, title, body, label1, label2, label3, num_comments, reddit_url),
        )
        post_count += 1

        # Comments start at column index 7
        cidx = 1
        for col_i in range(7, len(row)):
            val = row.iloc[col_i]
            if pd.notna(val) and str(val).strip():
                conn.execute(
                    "INSERT INTO comments (post_id, comment_index, text) VALUES (?,?,?)",
                    (post_id, cidx, str(val).strip()),
                )
                cidx += 1
                comment_count += 1

    conn.commit()
    conn.close()
    print(f"  Loaded {post_count} posts, {comment_count} comments")


def load_llm_outputs():
    path = os.path.join(DATA_DIR, "PostLevel_Outputs.xlsx")
    if not os.path.exists(path):
        print("PostLevel_Outputs.xlsx not found, skipping LLM outputs")
        return

    print(f"Loading {path} ...")
    df = pd.read_excel(path, engine="openpyxl")

    conn = get_db()
    count = 0
    for _, row in df.iterrows():
        post_id = str(row.get("post_id", ""))
        if not post_id:
            continue
        conn.execute(
            """INSERT INTO llm_outputs
                (post_id, model_family, model_name, summary, unique_advice_json,
                 divergences_json, clinically_relevant_notes_json, data_quality)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                post_id,
                str(row.get("model_family", "")) if pd.notna(row.get("model_family")) else None,
                str(row.get("model_name", "")) if pd.notna(row.get("model_name")) else None,
                str(row.get("summary", "")) if pd.notna(row.get("summary")) else None,
                str(row.get("unique_advice_json", "")) if pd.notna(row.get("unique_advice_json")) else None,
                str(row.get("divergences_json", "")) if pd.notna(row.get("divergences_json")) else None,
                str(row.get("clinically_relevant_notes_json", "")) if pd.notna(row.get("clinically_relevant_notes_json")) else None,
                str(row.get("data_quality", "")) if pd.notna(row.get("data_quality")) else None,
            ),
        )
        count += 1

    conn.commit()
    conn.close()
    print(f"  Loaded {count} LLM output rows")


if __name__ == "__main__":
    print("Initializing database...")
    init_db()
    load_6k_data()
    load_llm_outputs()
    print("Done!")
