"""
Extract the next 20 posts per class from the 6K dataset,
format them to match the pipeline input, and save as a new input Excel.
"""

import pandas as pd
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
FULL_DATA = SCRIPT_DIR / "6K_data_with_comments (1).xlsx"
ORIGINAL_INPUT = SCRIPT_DIR / "Top10_High_With_Text_And_Comments.xlsx"
OUTPUT = SCRIPT_DIR / "Next20_Per_Class_Input.xlsx"

NEXT_N = 20  # how many new posts per class

# Load datasets
df_full = pd.read_excel(FULL_DATA)
df_orig = pd.read_excel(ORIGINAL_INPUT)

# Normalize class labels
df_full["class_label"] = df_full["Label1"].replace(
    "Psychophysical Effects", "Psycho-Physical Effects"
)

# IDs already processed
orig_ids = set(df_orig["post_id"].astype(str).tolist())

# Combine Comment1..Comment10 into a single text column matching pipeline format
def combine_comments(row):
    parts = []
    for i in range(1, 11):
        col = f"Comment{i}"
        val = row.get(col)
        if pd.notna(val) and str(val).strip():
            parts.append(f"C{i}: {str(val).strip()}")
    return "\n\n".join(parts)

df_full["top_level_comments_text"] = df_full.apply(combine_comments, axis=1)

# For each class: sort by engagement (number_top_level_comment desc),
# exclude already-processed, take next N
frames = []
for cls in sorted(df_full["class_label"].unique()):
    sub = df_full[df_full["class_label"] == cls].sort_values(
        "number_top_level_comment", ascending=False
    )
    remaining = sub[~sub["id"].astype(str).isin(orig_ids)]
    batch = remaining.head(NEXT_N).copy()
    batch["rank_in_group"] = range(11, 11 + len(batch))
    batch["engagement_level"] = "High"
    frames.append(batch)
    print(f"{cls}: selected {len(batch)} posts (comments range: "
          f"{batch['number_top_level_comment'].max()}-{batch['number_top_level_comment'].min()})")

result = pd.concat(frames, ignore_index=True)

# Rename/select columns to match pipeline input format
result = result.rename(columns={
    "id": "post_id",
    "body": "post_text",
})

# Build link from post_id (Reddit shortlink)
result["link"] = "https://www.reddit.com/r/suboxone/comments/" + result["post_id"] + "/"

output_cols = [
    "class_label", "engagement_level", "rank_in_group",
    "post_id", "title", "number_top_level_comment", "link",
    "post_text", "top_level_comments_text",
]
result = result[output_cols].rename(columns={"number_top_level_comment": "top_level_comment_count"})

result.to_excel(OUTPUT, index=False)
print(f"\nSaved {len(result)} posts to {OUTPUT.name}")
print(f"Classes: {result['class_label'].value_counts().to_dict()}")
