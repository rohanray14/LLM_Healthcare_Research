"""
Combine original 10 + next 20 posts per class into a single input file.
The pipeline will skip already-processed posts and only analyze the new ones.
"""

import pandas as pd
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent

# Original 60 posts (already have text + comments)
df_orig = pd.read_excel(SCRIPT_DIR / "Top10_High_With_Text_And_Comments.xlsx")
# New 120 posts
df_new = pd.read_excel(SCRIPT_DIR / "Next20_Per_Class_Input.xlsx")

# Align columns
keep_cols = [
    "class_label", "engagement_level", "rank_in_group",
    "post_id", "title", "top_level_comment_count", "link",
    "post_text", "top_level_comments_text",
]

# Original may have extra columns (OpenAI Output, Claude Output) — drop them
for col in keep_cols:
    if col not in df_orig.columns:
        df_orig[col] = ""

df_orig_clean = df_orig[keep_cols]
df_new_clean = df_new[keep_cols]

combined = pd.concat([df_orig_clean, df_new_clean], ignore_index=True)

output_path = SCRIPT_DIR / "Combined_30_Per_Class_Input.xlsx"
combined.to_excel(output_path, index=False)

print(f"Combined: {len(combined)} posts ({len(df_orig_clean)} original + {len(df_new_clean)} new)")
print(f"Per class:")
print(combined["class_label"].value_counts().sort_index().to_string())
print(f"\nSaved to: {output_path.name}")
