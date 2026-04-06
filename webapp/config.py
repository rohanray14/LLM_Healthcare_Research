import os

SECRET_KEY = os.environ.get("SECRET_KEY", "change-me-in-production-abc123")
DATABASE_PATH = os.environ.get("DATABASE_PATH", os.path.join(os.path.dirname(__file__), "verification.db"))
DATA_DIR = os.environ.get("DATA_DIR", os.path.dirname(os.path.dirname(__file__)))

USERS = {
    "dr_smith": "password123",
    "dr_jones": "password456",
    "admin": "admin123",
}

LABELS = [
    "Access Logistics",
    "Co-occuring Drug Usage",
    "MOUD Administration",
    "Others",
    "Psycho-Physical Effects",
    "Psychophysical Effects",
    "Tapering",
]
