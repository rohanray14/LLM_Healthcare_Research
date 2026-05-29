from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash

db = SQLAlchemy()


class Expert(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(256), nullable=True)
    reviews = db.relationship("ItemReview", backref="expert", lazy=True)
    annotations = db.relationship("TextAnnotation", backref="expert", lazy=True)
    assignments = db.relationship("Assignment", backref="expert", lazy=True)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


class Assignment(db.Model):
    """Maps an expert to a specific post they are assigned to review."""
    id = db.Column(db.Integer, primary_key=True)
    expert_id = db.Column(db.Integer, db.ForeignKey("expert.id"), nullable=False)
    post_id = db.Column(db.String(20), nullable=False)

    __table_args__ = (
        db.UniqueConstraint("expert_id", "post_id"),
    )


class ItemReview(db.Model):
    """Verdict on an individual item (advice #2, divergence #1, clinical note #3, etc.)."""
    id = db.Column(db.Integer, primary_key=True)
    expert_id = db.Column(db.Integer, db.ForeignKey("expert.id"), nullable=False)
    post_id = db.Column(db.String(20), nullable=False)
    model_name = db.Column(db.String(60), nullable=False)
    section = db.Column(db.String(40), nullable=False)      # summary, advice, divergence, clinical, quality
    item_index = db.Column(db.Integer, nullable=False)       # 0-based index within section
    verdict = db.Column(db.String(20), nullable=True)        # correct / incorrect / not_sure
    note = db.Column(db.Text, default="")
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), onupdate=db.func.now())

    __table_args__ = (
        db.UniqueConstraint("expert_id", "post_id", "model_name", "section", "item_index"),
    )


class TextAnnotation(db.Model):
    """Highlighted text annotation on paragraph sections (summary, data_quality)."""
    id = db.Column(db.Integer, primary_key=True)
    expert_id = db.Column(db.Integer, db.ForeignKey("expert.id"), nullable=False)
    post_id = db.Column(db.String(20), nullable=False)
    model_name = db.Column(db.String(60), nullable=False)
    section = db.Column(db.String(40), nullable=False)
    item_index = db.Column(db.Integer, nullable=False, default=0)
    start_offset = db.Column(db.Integer, nullable=False)
    end_offset = db.Column(db.Integer, nullable=False)
    highlighted_text = db.Column(db.Text, nullable=False)
    annotation_text = db.Column(db.Text, default="")
    verdict = db.Column(db.String(20), nullable=True)        # correct / incorrect / not_sure
    created_at = db.Column(db.DateTime, server_default=db.func.now())
