from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash

db = SQLAlchemy()


class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(256), nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


class Project(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    slug = db.Column(db.String(120), unique=True, nullable=False)
    name = db.Column(db.String(200), nullable=False)
    config_json = db.Column(db.Text, nullable=False, default="{}")
    owner_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    owner = db.relationship("User", backref="owned_projects")


class ProjectMember(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.Integer, db.ForeignKey("project.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    role = db.Column(db.String(20), nullable=False, default="annotator")  # "admin" or "annotator"
    project = db.relationship("Project", backref="members")
    user = db.relationship("User", backref="memberships")
    __table_args__ = (db.UniqueConstraint("project_id", "user_id"),)


class Assignment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.Integer, db.ForeignKey("project.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    post_id = db.Column(db.String(120), nullable=False)
    __table_args__ = (db.UniqueConstraint("project_id", "user_id", "post_id"),)


class TextAnnotation(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.Integer, db.ForeignKey("project.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    post_id = db.Column(db.String(120), nullable=False)
    model_name = db.Column(db.String(60), nullable=False)
    section = db.Column(db.String(40), nullable=False)
    item_index = db.Column(db.Integer, nullable=False, default=0)
    start_offset = db.Column(db.Integer, nullable=False)
    end_offset = db.Column(db.Integer, nullable=False)
    highlighted_text = db.Column(db.Text, nullable=False)
    annotation_text = db.Column(db.Text, default="")
    verdict = db.Column(db.String(20), nullable=True)
    harm_verdict = db.Column(db.String(20), nullable=True)
    factual_reasoning = db.Column(db.Text, default="")
    harm_reasoning = db.Column(db.Text, default="")
    is_gt_span = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    user = db.relationship("User")
