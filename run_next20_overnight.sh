#!/bin/bash
# Run post-level analysis on 30 posts/class (skips the 60 already done).
# Appends results to the existing PostLevel_Outputs.xlsx.
# Usage: nohup ./run_next20_overnight.sh &
#   or:  ./run_next20_overnight.sh

cd "$(dirname "$0")"

export INPUT_EXCEL="Combined_30_Per_Class_Input.xlsx"
export POST_LEVEL_OUTPUT="PostLevel_Outputs.xlsx"
export CLASS_LEVEL_OUTPUT="ClassLevel_Summaries.xlsx"
export CROSS_CLASS_MD="CrossClass_Report.md"
export CROSS_CLASS_JSON="CrossClass_Report.json"

echo "=== Starting at $(date) ==="
echo "Input: $INPUT_EXCEL (180 posts, 30/class)"
echo "Output: $POST_LEVEL_OUTPUT (will skip already-processed posts)"
echo ""

.venv/bin/python3 run_full_pipeline.py 2>&1 | tee next20_run.log

echo "=== Finished at $(date) ==="
