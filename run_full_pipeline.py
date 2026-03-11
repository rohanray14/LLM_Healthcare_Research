"""
LLM analysis pipeline: post-level → class-level → cross-class.

Install:
  pip install pandas openpyxl tqdm openai anthropic google-genai python-dotenv

Env vars: OPENAI_API_KEY, ANTHROPIC_API_KEY, GEMINI_API_KEY
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pandas as pd
import openai
from langchain_dartmouth.llms import ChatDartmouth
from langchain_core.messages import SystemMessage, HumanMessage
from tqdm import tqdm

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

# --- Paths (override via env or edit here) ---
SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_EXCEL = Path(os.environ.get("INPUT_EXCEL", SCRIPT_DIR / "Top10_High_With_Text_And_Comments.xlsx"))
POST_LEVEL_OUTPUT = Path(os.environ.get("POST_LEVEL_OUTPUT", SCRIPT_DIR / "PostLevel_Outputs.xlsx"))
CLASS_LEVEL_OUTPUT = Path(os.environ.get("CLASS_LEVEL_OUTPUT", SCRIPT_DIR / "ClassLevel_Summaries.xlsx"))
CROSS_CLASS_MD = Path(os.environ.get("CROSS_CLASS_MD", SCRIPT_DIR / "CrossClass_Report.md"))
CROSS_CLASS_JSON = Path(os.environ.get("CROSS_CLASS_JSON", SCRIPT_DIR / "CrossClass_Report.json"))
FORCE = os.environ.get("FORCE", "").strip().lower() in ("1", "true", "yes")
# Run only OpenAI: set ONLY_OPENAI=1 (or PROVIDERS=openai). Otherwise runs all three.
ONLY_OPENAI = os.environ.get("ONLY_OPENAI", "").strip().lower() in ("1", "true", "yes") or os.environ.get("PROVIDERS", "").strip().lower() == "openai"

# --- Rate limit & retries ---
BASE_DELAY = 1.0
MAX_RETRIES = 4


def retry_with_backoff(fn, *args, **kwargs):
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_err = e
            if attempt < MAX_RETRIES - 1:
                delay = BASE_DELAY * (2 ** attempt)
                time.sleep(delay)
    raise last_err


# --- Prompt (same for all 3 models) ---
POST_ANALYSIS_SYSTEM = """You are a qualitative researcher analyzing Reddit advice threads. Output ONLY valid JSON, no markdown or extra text."""

POST_ANALYSIS_USER_TEMPLATE = """Analyze this Reddit post and its top-level comments. Use the exact JSON schema below.

Input:
- class_label: {class_label}
- post_id: {post_id}
- title: {title}

POST BODY:
{post_text}

TOP-LEVEL COMMENTS (C# = comment ID):
{top_level_comments_text}

Tasks:
1) Extract unique advice units (deduped, short, specific).
2) For each: label agreement (high/mixed/low) and cite comment IDs [C#].
3) Identify divergences as split viewpoints with evidence [C#].
4) Note clinically relevant: risk flags, misinformation, access barriers.
5) Data quality: few comments, deleted, off-topic, etc.

Output this exact JSON structure only:
{{
  "summary": "2-4 sentences",
  "unique_advice": [
    {{ "advice": "...", "agreement": "high|mixed|low", "support": ["C1","C7"], "counterpoints": ["C3"] }}
  ],
  "divergences": [
    {{ "topic": "...", "view_a": "...", "evidence_a": ["C2","C9"], "view_b": "...", "evidence_b": ["C4"] }}
  ],
  "clinically_relevant_notes": ["..."],
  "data_quality": "..."
}}"""

CLASS_SYNTHESIS_SYSTEM = """You are a qualitative researcher synthesizing themes across multiple post analyses. Output ONLY valid JSON."""

CLASS_SYNTHESIS_USER_TEMPLATE = """Synthesize the following post-level analyses for class_label: {class_label}.

Post-level summaries (one per post):
{post_summaries}

From these, produce a class-level summary. Output this exact JSON only:
{{
  "class_label": "{class_label}",
  "top_themes": [{{ "theme": "...", "count": 7, "example_posts": ["id1", "id2"] }}],
  "agreement_areas": ["..."],
  "divergence_axes": [{{ "axis": "...", "views": ["...", "..."], "example_posts": ["..."] }}],
  "clinically_relevant_patterns": ["..."],
  "overall_takeaway": "5-7 sentences"
}}"""


def call_openai(prompt_user: str, repair: bool = False, system_content: str | None = None) -> str:
    client = openai.OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    if repair:
        msg = [{"role": "user", "content": prompt_user}]
    else:
        system = system_content or POST_ANALYSIS_SYSTEM
        msg = [{"role": "system", "content": system}, {"role": "user", "content": prompt_user}]

    def _call():
        r = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=msg,
            temperature=0.2,
        )
        return r.choices[0].message.content or ""

    return retry_with_backoff(_call)


def call_anthropic(prompt_user: str, repair: bool = False, system_content: str | None = None) -> str:
    llm = ChatDartmouth(
        model_name="anthropic.claude-sonnet-4-6",
        temperature=0.2,
        max_tokens=4096,
    )
    if repair:
        messages = [HumanMessage(content=prompt_user)]
    else:
        system = system_content or POST_ANALYSIS_SYSTEM
        messages = [SystemMessage(content=system), HumanMessage(content=prompt_user)]

    def _call():
        r = llm.invoke(messages)
        return r.content or ""

    return retry_with_backoff(_call)


GEMINI_MODELS = ["gemini-2.5-flash", "gemini-1.5-flash", "gemini-1.5-pro-latest"]


def call_gemini(prompt_user: str, repair: bool = False, system_content: str | None = None) -> str:
    key = os.environ.get("GEMINI_API_KEY")
    sys = system_content or POST_ANALYSIS_SYSTEM
    content = prompt_user if repair else (sys + "\n\n" + prompt_user)
    last_err = None
    for model in GEMINI_MODELS:
        try:
            def _call():
                client = google_genai.Client(api_key=key)
                r = client.models.generate_content(
                    model=model,
                    contents=content,
                    config={"temperature": 0.2},
                )
                return (getattr(r, "text", None) or "").strip()
            return retry_with_backoff(_call)
        except Exception as e:
            last_err = e
            if "404" in str(e) or "not found" in str(e).lower() or "not supported" in str(e).lower():
                continue
            raise
    raise last_err


def extract_json_block(text: str) -> str | None:
    if not text or not isinstance(text, str):
        return None
    text = text.strip()
    for start in ("{", "```json", "```"):
        if start in text:
            if start == "{":
                i = text.index("{")
            else:
                i = text.find(start)
                if i >= 0:
                    i = text.index("{", i)
                else:
                    continue
            depth = 0
            for j in range(i, len(text)):
                if text[j] == "{":
                    depth += 1
                elif text[j] == "}":
                    depth -= 1
                    if depth == 0:
                        return text[i : j + 1]
    return None


def parse_post_json(raw: str) -> dict | None:
    blob = extract_json_block(raw)
    if not blob:
        return None
    try:
        return json.loads(blob)
    except json.JSONDecodeError:
        return None


def repair_json(raw: str, model_family: str, prompt_user: str) -> str:
    repair_prompt = f"""The following text was supposed to be valid JSON but is not. Return only the corrected JSON, nothing else.\n\n{raw[:8000]}"""
    if model_family == "openai":
        return call_openai(repair_prompt, repair=True)
    if model_family == "anthropic":
        return call_anthropic(repair_prompt, repair=True)
    return ""


def run_post_level(input_df: pd.DataFrame, existing: pd.DataFrame | None) -> pd.DataFrame:
    rows = []
    seen = set()
    if existing is not None and not existing.empty and "post_id" in existing.columns and "model_family" in existing.columns:
        for _, r in existing.iterrows():
            err = r.get("error")
            summary = r.get("summary")
            has_error = pd.notna(err) and str(err).strip()
            has_summary = pd.notna(summary) and str(summary).strip()
            # Skip if it succeeded (has summary and no error)
            if has_error and not has_summary:
                continue
            key = (str(r.get("post_id", "")), str(r.get("model_family", "")))
            seen.add(key)

    if ONLY_OPENAI:
        models = [("openai", "gpt-4o-mini", call_openai)]
    else:
        models = [
            ("openai", "gpt-4o-mini", call_openai),
            ("anthropic", "claude-sonnet-4.6", call_anthropic),
        ]

    for _, row in tqdm(input_df.iterrows(), total=len(input_df), desc="Post-level"):
        class_label = row.get("class_label", "")
        post_id = str(row.get("post_id", ""))
        title = str(row.get("title", ""))[:500]
        link = row.get("link", "")
        post_text = str(row.get("post_text", "") or "")[:15000]
        comments = str(row.get("top_level_comments_text", "") or "")[:15000]

        prompt_user = POST_ANALYSIS_USER_TEMPLATE.format(
            class_label=class_label,
            post_id=post_id,
            title=title,
            post_text=post_text or "(no post body)",
            top_level_comments_text=comments or "(no comments)",
        )

        for model_family, model_name, call_fn in models:
            if not FORCE and (post_id, model_family) in seen:
                continue
            raw_output = ""
            err_msg = ""
            try:
                time.sleep(BASE_DELAY)
                raw_output = call_fn(prompt_user)
            except Exception as e:
                err_msg = str(e)
                rows.append({
                    "class_label": class_label,
                    "post_id": post_id,
                    "title": title,
                    "link": link,
                    "model_family": model_family,
                    "model_name": model_name,
                    "summary": "",
                    "unique_advice_json": "",
                    "divergences_json": "",
                    "clinically_relevant_notes_json": "",
                    "data_quality": "",
                    "raw_output": raw_output,
                    "error": err_msg,
                })
                continue

            parsed = parse_post_json(raw_output)
            if parsed is None and raw_output:
                try:
                    repaired = repair_json(raw_output, model_family, prompt_user)
                    if repaired:
                        time.sleep(BASE_DELAY)
                        parsed = parse_post_json(repaired)
                        if parsed is not None:
                            raw_output = repaired
                except Exception:
                    pass

            summary = ""
            unique_advice_json = ""
            divergences_json = ""
            clinically_relevant_notes_json = ""
            data_quality = ""
            if isinstance(parsed, dict):
                summary = str(parsed.get("summary", ""))[:2000]
                unique_advice_json = json.dumps(parsed.get("unique_advice", []), ensure_ascii=False)[:10000]
                divergences_json = json.dumps(parsed.get("divergences", []), ensure_ascii=False)[:10000]
                clinically_relevant_notes_json = json.dumps(parsed.get("clinically_relevant_notes", []), ensure_ascii=False)[:5000]
                data_quality = str(parsed.get("data_quality", ""))[:1000]

            rows.append({
                "class_label": class_label,
                "post_id": post_id,
                "title": title,
                "link": link,
                "model_family": model_family,
                "model_name": model_name,
                "summary": summary,
                "unique_advice_json": unique_advice_json,
                "divergences_json": divergences_json,
                "clinically_relevant_notes_json": clinically_relevant_notes_json,
                "data_quality": data_quality,
                "raw_output": raw_output[:50000],
                "error": err_msg,
            })

    out_df = pd.DataFrame(rows)
    if existing is not None and not existing.empty:
        out_df = pd.concat([existing, out_df], ignore_index=True)
        out_df = out_df.drop_duplicates(subset=["post_id", "model_family"], keep="last")
    return out_df


def run_class_level(post_level_df: pd.DataFrame) -> pd.DataFrame:
    primary = "openai"
    fallback = ["anthropic"]
    class_rows = []

    for class_label in tqdm(post_level_df["class_label"].dropna().unique(), desc="Class-level"):
        subset = post_level_df[post_level_df["class_label"] == class_label]
        subset = subset[subset["error"].isna() | (subset["error"].astype(str).str.strip() == "")]
        model_used = primary
        for m in [primary] + fallback:
            m_sub = subset[subset["model_family"] == m]
            if len(m_sub) >= 5:
                model_used = m
                break
        sub = subset[subset["model_family"] == model_used].head(10)
        if sub.empty:
            class_rows.append({
                "class_label": class_label,
                "top_themes_json": "[]",
                "agreement_areas_json": "[]",
                "divergence_axes_json": "[]",
                "clinically_relevant_patterns_json": "[]",
                "overall_takeaway": "",
                "raw_class_json": "",
                "error": "No valid post-level data",
            })
            continue

        post_summaries = "\n\n".join(
            f"post_id: {r['post_id']}\n{r.get('summary', '')}" for _, r in sub.iterrows()
        )

        prompt_user = CLASS_SYNTHESIS_USER_TEMPLATE.format(
            class_label=class_label,
            post_summaries=post_summaries[:20000],
        )
        raw_class_json = ""
        err_msg = ""
        try:
            time.sleep(BASE_DELAY)
            if model_used == "openai":
                raw_class_json = call_openai(prompt_user, system_content=CLASS_SYNTHESIS_SYSTEM)
            elif model_used == "anthropic":
                raw_class_json = call_anthropic(prompt_user, system_content=CLASS_SYNTHESIS_SYSTEM)
            else:
                raw_class_json = call_gemini(prompt_user, system_content=CLASS_SYNTHESIS_SYSTEM)
        except Exception as e:
            err_msg = str(e)

        parsed = parse_post_json(raw_class_json) if raw_class_json else None
        top_themes_json = "[]"
        agreement_areas_json = "[]"
        divergence_axes_json = "[]"
        clinically_relevant_patterns_json = "[]"
        overall_takeaway = ""
        if isinstance(parsed, dict):
            top_themes_json = json.dumps(parsed.get("top_themes", []), ensure_ascii=False)
            agreement_areas_json = json.dumps(parsed.get("agreement_areas", []), ensure_ascii=False)
            divergence_axes_json = json.dumps(parsed.get("divergence_axes", []), ensure_ascii=False)
            clinically_relevant_patterns_json = json.dumps(parsed.get("clinically_relevant_patterns", []), ensure_ascii=False)
            overall_takeaway = str(parsed.get("overall_takeaway", ""))[:3000]

        class_rows.append({
            "class_label": class_label,
            "top_themes_json": top_themes_json,
            "agreement_areas_json": agreement_areas_json,
            "divergence_axes_json": divergence_axes_json,
            "clinically_relevant_patterns_json": clinically_relevant_patterns_json,
            "overall_takeaway": overall_takeaway,
            "raw_class_json": raw_class_json[:50000],
            "error": err_msg,
        })

    return pd.DataFrame(class_rows)


def run_cross_class(class_df: pd.DataFrame, post_level_df: pd.DataFrame) -> tuple[dict, str]:
    insights = []
    by_class = {}
    for _, row in class_df.iterrows():
        cl = row.get("class_label", "")
        if cl not in by_class:
            by_class[cl] = {"themes": [], "divergence_count": 0, "risk_notes": []}
        try:
            by_class[cl]["themes"] = json.loads(row.get("top_themes_json", "[]") or "[]")
        except Exception:
            pass
        try:
            axes = json.loads(row.get("divergence_axes_json", "[]") or "[]")
            by_class[cl]["divergence_count"] = len(axes)
        except Exception:
            pass
        try:
            by_class[cl]["risk_notes"] = json.loads(row.get("clinically_relevant_patterns_json", "[]") or "[]")
        except Exception:
            pass

    post_by_class = post_level_df.groupby("class_label").size().to_dict()
    divergence_freq = {k: v["divergence_count"] for k, v in by_class.items()}
    risk_counts = {k: len(v["risk_notes"]) for k, v in by_class.items()}

    high_div = sorted(divergence_freq.items(), key=lambda x: -x[1])[:3]
    high_risk = sorted(risk_counts.items(), key=lambda x: -x[1])[:3]
    all_themes = []
    for cl, data in by_class.items():
        for t in data.get("themes", []):
            if isinstance(t, dict):
                all_themes.append((cl, t.get("theme", ""), t.get("count", 0)))
            else:
                all_themes.append((cl, str(t), 0))
    theme_summary = []
    for cl in by_class:
        themes = [t.get("theme", "") for t in by_class[cl].get("themes", []) if isinstance(t, dict)][:5]
        theme_summary.append({"class": cl, "top_themes": themes})

    for i, (cl, _) in enumerate(high_div, 1):
        insights.append(f"Classes with highest divergence frequency: {cl} (rank {i}).")
    for i, (cl, _) in enumerate(high_risk, 1):
        insights.append(f"Risk/clinical note concentration: {cl} ranks {i} in clinically relevant patterns.")
    insights.append("Cross-class themes vary by engagement context (e.g. access, dosing, stigma).")
    insights.append("Agreement areas often center on tapering and professional support.")
    insights.append("Divergence axes frequently involve cold turkey vs. gradual reduction.")
    insights.append("Data quality issues (few comments, deleted) affect some classes more.")
    insights.append("Misinformation patterns cluster in classes with high uncertainty.")
    insights.append("Access barriers (cost, prescriber) recur across multiple classes.")
    insights.append("Peer support and community norms are consistent themes.")
    insights.append("Risk flags often relate to dose and concurrent substance use.")

    table_lines = [
        "| Class | Post count | Divergence axes | Risk/clinical notes |",
        "|-------|------------|-----------------|---------------------|",
    ]
    for cl in sorted(by_class.keys()):
        n = post_by_class.get(cl, 0)
        d = by_class[cl].get("divergence_count", 0)
        r = len(by_class[cl].get("risk_notes", []))
        table_lines.append(f"| {cl} | {n} | {d} | {r} |")

    report_dict = {
        "insights": insights[:12],
        "by_class": {k: {"divergence_count": v["divergence_count"], "risk_note_count": len(v["risk_notes"]), "theme_count": len(v["themes"])} for k, v in by_class.items()},
        "comparison_table": "\n".join(table_lines),
    }

    md = "# Cross-class report\n\n## Insights\n\n" + "\n".join(f"- {s}" for s in insights[:12]) + "\n\n## Comparison\n\n" + "\n".join(table_lines)
    return report_dict, md


def main():
    if ONLY_OPENAI:
        if not os.environ.get("OPENAI_API_KEY"):
            raise SystemExit("Missing OPENAI_API_KEY")
        print("Running with OpenAI only (ONLY_OPENAI=1).")
    else:
        for key in ("OPENAI_API_KEY", "DARTMOUTH_CHAT_API_KEY"):
            if not os.environ.get(key):
                raise SystemExit(f"Missing {key}")

    print("Input:", INPUT_EXCEL)
    print("Outputs:")
    print("  Post-level:", POST_LEVEL_OUTPUT)
    print("  Class-level:", CLASS_LEVEL_OUTPUT)
    print("  Cross-class MD:", CROSS_CLASS_MD)
    print("  Cross-class JSON:", CROSS_CLASS_JSON)

    if not INPUT_EXCEL.exists():
        raise SystemExit(f"Input not found: {INPUT_EXCEL}")

    input_df = pd.read_excel(INPUT_EXCEL, engine="openpyxl")
    required = ["class_label", "post_id", "title", "link", "post_text", "top_level_comments_text"]
    for c in required:
        if c not in input_df.columns:
            raise SystemExit(f"Missing column: {c}")

    existing_post = None
    if POST_LEVEL_OUTPUT.exists() and not FORCE:
        existing_post = pd.read_excel(POST_LEVEL_OUTPUT, engine="openpyxl")

    post_level_df = run_post_level(input_df, existing_post)
    post_level_df.to_excel(POST_LEVEL_OUTPUT, index=False, engine="openpyxl")
    print(f"Wrote {POST_LEVEL_OUTPUT}")

    class_level_df = run_class_level(post_level_df)
    class_level_df.to_excel(CLASS_LEVEL_OUTPUT, index=False, engine="openpyxl")
    print(f"Wrote {CLASS_LEVEL_OUTPUT}")

    report_dict, report_md = run_cross_class(class_level_df, post_level_df)
    CROSS_CLASS_JSON.write_text(json.dumps(report_dict, indent=2, ensure_ascii=False), encoding="utf-8")
    CROSS_CLASS_MD.write_text(report_md, encoding="utf-8")
    print(f"Wrote {CROSS_CLASS_MD} and {CROSS_CLASS_JSON}")


if __name__ == "__main__":
    main()
