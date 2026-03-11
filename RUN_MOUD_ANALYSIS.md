# Agentic MOUD Analysis

**Step 1:** `run_moud_analysis.py` — Runs your MOUD peer-response prompt on each post. Writes to:
- Column J: **OpenAI Output**
- Column K: **Claude Output**

**Step 2:** `run_class_synthesis.py` — Groups posts by class, takes all post-level outputs per class, and runs the class-level pattern synthesis prompt. Writes results to a new sheet **ClassLevel_Synthesis** in the same Excel.

## Setup

1. **API key:** Add `OPENAI_API_KEY` to `.env` (see [API_KEYS_AND_BILLING.md](API_KEYS_AND_BILLING.md)).
2. **Activate venv:** `source .venv/bin/activate`

## Usage

```bash
# Process only rows missing output (~35 remaining)
python run_moud_analysis.py

# Process all 60 rows (overwrites existing)
python run_moud_analysis.py --all

# Dry run (see what would run, no API calls)
python run_moud_analysis.py --dry-run

# Process first 5 rows only (for testing)
python run_moud_analysis.py --limit 5

# Claude only (fill column K, skip OpenAI)
python run_moud_analysis.py --provider claude

# OpenAI only
python run_moud_analysis.py --provider openai
```

### Class-level synthesis (run after post-level is complete)

```bash
# Process all classes (groups by class_label, uses whatever post-level outputs exist)
python run_class_synthesis.py

# Dry run
python run_class_synthesis.py --dry-run

# Process one class only
python run_class_synthesis.py --class "Access Logistics"
```

## What it does

- Reads `post_text` and `top_level_comments_text` from each row
- Uses your exact prompt with `class_label` from the Excel
- Calls OpenAI `gpt-4o-mini`
- Writes the response to the **OpenAI Output** column
- Saves after each row (so you can stop and resume)
- 1 second delay between calls (rate limiting)

**Class synthesis** (`run_class_synthesis.py`):
- Groups posts by `class_label`
- For each class, concatenates all post-level outputs and runs your Universal Class-Level Pattern Synthesis prompt
- Writes to a new sheet **ClassLevel_Synthesis** (columns: class_label, Class_Level_Synthesis)
- Processes every class that has at least one post-level output
