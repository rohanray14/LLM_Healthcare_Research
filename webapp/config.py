import os

SECRET_KEY = os.environ.get("SECRET_KEY", "change-me-in-production-abc123")
DATABASE_URL = os.environ.get("DATABASE_URL")  # PostgreSQL on Render
DATABASE_PATH = os.environ.get("DATABASE_PATH", os.path.join(os.path.dirname(__file__), "verification.db"))
DATA_DIR = os.environ.get("DATA_DIR", os.path.dirname(os.path.dirname(__file__)))
USE_POSTGRES = DATABASE_URL is not None

USERS = {
    "dr_smith": {"password": "password123", "role": "expert"},
    "dr_jones": {"password": "password456", "role": "expert"},
    "admin": {"password": "admin123", "role": "expert"},
    "annotator1": {"password": "annotate123", "role": "annotator"},
    "annotator2": {"password": "annotate456", "role": "annotator"},
}

LABELS = [
    "Access Logistics",
    "Co-occuring Drug Usage",
    "MOUD Administration",
    "Others",
    "Psychophysical Effects",
    "Tapering",
]

# Comment-level codes for non-expert annotation
COMMENT_CODES = {
    "CLAIM": {
        "label": "Confident Claim",
        "description": "Any assertion presented with confidence — whether a positive fact or a directive warning. No hedging, no personal frame.",
        "example": '"You don\'t need to wait." / "Never take benzos with subs."',
        "triggers_spans": True,
        "color": "#fee2e2",
        "border_color": "#ef4444",
    },
    "EXPER": {
        "label": "Personal Experience",
        "description": "Explicitly framed as what happened to the poster personally. May include procedural detail or taper schedules drawn from own experience.",
        "example": '"When I tapered I went 8→4→2→1mg and it worked for me."',
        "triggers_spans": False,
        "color": "#dbeafe",
        "border_color": "#3b82f6",
    },
    "HEDGED": {
        "label": "Hedged Claim",
        "description": "Uncertainty explicitly signaled by the poster. The claim is softened, qualified, or presented as uncertain.",
        "example": '"I think maybe waiting a day is safer, but I\'m not totally sure."',
        "triggers_spans": False,
        "color": "#fef9c3",
        "border_color": "#eab308",
    },
    "SUPPORT": {
        "label": "Emotional Support",
        "description": "No clinical claim. Validates, encourages, empathizes, or normalizes the poster's experience.",
        "example": '"You\'re not cheating sobriety. You got this."',
        "triggers_spans": False,
        "color": "#dcfce7",
        "border_color": "#22c55e",
    },
    "REF": {
        "label": "Referral",
        "description": "Directs the poster to a provider, clinic, pharmacist, or authoritative resource for guidance.",
        "example": '"Please call your prescriber before trying this."',
        "triggers_spans": False,
        "color": "#e0e7ff",
        "border_color": "#6366f1",
    },
    "META-R": {
        "label": "Reaction / Advocacy",
        "description": "Emotional reaction, shared distress, or explicit advocacy against stigma or policy. No clinical advice directed at OP.",
        "example": '"This system is broken.", "Your doctor is an idiot.", "Bullshit rules."',
        "triggers_spans": False,
        "color": "#f3e8ff",
        "border_color": "#a855f7",
    },
}
