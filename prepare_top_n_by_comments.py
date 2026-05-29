"""
Select top N posts per class from the 6K dataset, ranked by number of comments.
Outputs an Excel file ready for run_full_pipeline.py or run_moud_analysis.py.

Usage:
  python prepare_top_n_by_comments.py              # default: top 30
  python prepare_top_n_by_comments.py --top 20     # top 20 per class
  python prepare_top_n_by_comments.py --top 30     # top 30 per class
"""

import argparse
import pandas as pd
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
FULL_DATA = SCRIPT_DIR / "6K_data_with_comments (1).xlsx"


def combine_comments(row):
    parts = []
    for i in range(1, 11):
        col = f"Comment{i}"
        val = row.get(col)
        if pd.notna(val) and str(val).strip():
            parts.append(f"C{i}: {str(val).strip()}")
    return "\n\n".join(parts)


def main():
    parser = argparse.ArgumentParser(description="Select top N posts per class by comment count")
    parser.add_argument("--top", type=int, default=30, help="Number of top posts per class (default: 30)")
    args = parser.parse_args()
    top_n = args.top

    df_full = pd.read_excel(FULL_DATA)
    df_full["class_label"] = df_full["Label1"].replace(
        "Psychophysical Effects", "Psycho-Physical Effects"
    )
    df_full["top_level_comments_text"] = df_full.apply(combine_comments, axis=1)

    frames = []
    for cls in sorted(df_full["class_label"].unique()):
        sub = df_full[df_full["class_label"] == cls].sort_values(
            "number_top_level_comment", ascending=False
        )
        batch = sub.head(top_n).copy()
        batch["rank_in_group"] = range(1, 1 + len(batch))
        batch["engagement_level"] = "High"
        frames.append(batch)
        print(f"{cls}: {len(batch)} posts (comments range: "
              f"{batch['number_top_level_comment'].max()}-{batch['number_top_level_comment'].min()})")

    result = pd.concat(frames, ignore_index=True)
    result = result.rename(columns={
        "id": "post_id",
        "body": "post_text",
    })
    result["link"] = "https://www.reddit.com/r/suboxone/comments/" + result["post_id"] + "/"

    output_cols = [
        "class_label", "engagement_level", "rank_in_group",
        "post_id", "title", "number_top_level_comment", "link",
        "post_text", "top_level_comments_text",
    ]
    result = result[output_cols].rename(columns={"number_top_level_comment": "top_level_comment_count"})

    output_path = SCRIPT_DIR / f"Top{top_n}_By_Comments_Input.xlsx"
    result.to_excel(output_path, index=False)
    print(f"\nSaved {len(result)} posts to {output_path.name}")
    print(f"Classes: {result['class_label'].value_counts().to_dict()}")


if __name__ == "__main__":
    main()
