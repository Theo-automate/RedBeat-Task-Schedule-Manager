from __future__ import annotations

from flask import Blueprint, render_template


def create_blueprint() -> Blueprint:
    """Return the RedBeat Schedule Manager Flask Blueprint.

    This is a placeholder that wires templates/static later. Users can
    register it in their Flask app via:

        app.register_blueprint(create_blueprint(), url_prefix="/redbeat")
    """

    bp = Blueprint(
        "redbeat",
        __name__,
        template_folder="templates",
        static_folder="static",
    )

    @bp.route("/")
    def scheduler_ui():
        return render_template("scheduler.html")

    return bp
