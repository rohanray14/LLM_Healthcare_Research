from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash

db = SQLAlchemy()


class Expert(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(256), nullable=True)
    password_plain = db.Column(db.String(256), nullable=True)
    annotations = db.relationship("TextAnnotation", backref="expert", lazy=True)
    assignments = db.relationship("Assignment", backref="expert", lazy=True)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


class Assignment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    expert_id = db.Column(db.Integer, db.ForeignKey("expert.id"), nullable=False)
    post_id = db.Column(db.String(20), nullable=False)

    __table_args__ = (
        db.UniqueConstraint("expert_id", "post_id"),
    )


class CommentCode(db.Model):
    """Comment-level codes (EXPER, HEDGED, CLAIM, etc.)."""
    id = db.Column(db.Integer, primary_key=True)
    expert_id = db.Column(db.Integer, db.ForeignKey("expert.id"), nullable=False)
    post_id = db.Column(db.String(20), nullable=False)
    comment_index = db.Column(db.Integer, nullable=False)
    code = db.Column(db.String(20), nullable=False)
    reason = db.Column(db.Text, default="")

    __table_args__ = (
        db.UniqueConstraint("expert_id", "post_id", "comment_index", "code"),
    )


class TextAnnotation(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    expert_id = db.Column(db.Integer, db.ForeignKey("expert.id"), nullable=False)
    post_id = db.Column(db.String(20), nullable=False)
    item_index = db.Column(db.Integer, nullable=False, default=0)
    start_offset = db.Column(db.Integer, nullable=False)
    end_offset = db.Column(db.Integer, nullable=False)
    highlighted_text = db.Column(db.Text, nullable=False)
    annotation_text = db.Column(db.Text, default="")
    verdict = db.Column(db.String(20), nullable=True)
    harm_verdict = db.Column(db.String(20), nullable=True)
    factual_reasoning = db.Column(db.Text, default="")
    harm_reasoning = db.Column(db.Text, default="")
    created_at = db.Column(db.DateTime, server_default=db.func.now())
