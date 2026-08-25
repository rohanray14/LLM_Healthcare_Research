import json
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash

db = SQLAlchemy()


class Project(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    slug = db.Column(db.String(100), unique=True, nullable=False)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    config_json = db.Column(db.Text, default="{}")

    posts = db.relationship("Post", backref="project", lazy=True, cascade="all, delete-orphan")
    experts = db.relationship("Expert", backref="project", lazy=True, cascade="all, delete-orphan")

    def get_config(self):
        return json.loads(self.config_json or "{}")

    def set_config(self, config):
        self.config_json = json.dumps(config)


class Post(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.Integer, db.ForeignKey("project.id"), nullable=False)
    post_id = db.Column(db.String(100), nullable=False)
    title = db.Column(db.Text, default="")
    body = db.Column(db.Text, default="")
    labels = db.Column(db.String(200), default="")
    split = db.Column(db.String(50), default="")
    link = db.Column(db.Text, default="")
    order_index = db.Column(db.Integer, default=0)
    pre_annotations_json = db.Column(db.Text, default="{}")

    comments = db.relationship("Comment", backref="post", lazy=True,
                               cascade="all, delete-orphan", order_by="Comment.comment_index")

    __table_args__ = (db.UniqueConstraint("project_id", "post_id"),)

    def get_pre_annotations(self):
        raw = json.loads(self.pre_annotations_json or "{}")
        return {int(k): v for k, v in raw.items()}


class Comment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    post_ref_id = db.Column(db.Integer, db.ForeignKey("post.id"), nullable=False)
    comment_index = db.Column(db.Integer, nullable=False)
    comment_id = db.Column(db.String(100), default="")
    comment_body = db.Column(db.Text, default="")


class Expert(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.Integer, db.ForeignKey("project.id"), nullable=False)
    username = db.Column(db.String(80), nullable=False)
    password_hash = db.Column(db.String(256), nullable=True)
    password_plain = db.Column(db.String(256), nullable=True)

    __table_args__ = (db.UniqueConstraint("project_id", "username"),)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


class Assignment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    expert_id = db.Column(db.Integer, db.ForeignKey("expert.id"), nullable=False)
    post_id = db.Column(db.String(100), nullable=False)

    __table_args__ = (db.UniqueConstraint("expert_id", "post_id"),)


class CommentCode(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    expert_id = db.Column(db.Integer, db.ForeignKey("expert.id"), nullable=False)
    post_id = db.Column(db.String(100), nullable=False)
    comment_index = db.Column(db.Integer, nullable=False)
    code = db.Column(db.String(50), nullable=False)
    reason = db.Column(db.Text, default="")

    __table_args__ = (db.UniqueConstraint("expert_id", "post_id", "comment_index", "code"),)


class SpanAnnotation(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    expert_id = db.Column(db.Integer, db.ForeignKey("expert.id"), nullable=False)
    post_id = db.Column(db.String(100), nullable=False)
    item_index = db.Column(db.Integer, nullable=False, default=0)
    start_offset = db.Column(db.Integer, nullable=False)
    end_offset = db.Column(db.Integer, nullable=False)
    highlighted_text = db.Column(db.Text, nullable=False)
    span_type = db.Column(db.String(50), default="CLAIM")
    created_at = db.Column(db.DateTime, server_default=db.func.now())


class PreSeedLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    expert_id = db.Column(db.Integer, db.ForeignKey("expert.id"), nullable=False)
    post_id = db.Column(db.String(100), nullable=False)

    __table_args__ = (db.UniqueConstraint("expert_id", "post_id"),)
