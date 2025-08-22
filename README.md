# RedBeat Schedule Manager

A Flask UI to manage Celery RedBeat schedules: browse, create, edit, delete, import/export.

## Installation
```bash
pip install redbeat-schedule-manager
```

## Quick start
```python
from flask import Flask
from redbeat_schedule_manager import create_blueprint

app = Flask(__name__)
app.register_blueprint(create_blueprint(), url_prefix="/redbeat")

if __name__ == "__main__":
	app.run(debug=True)
```

Visit `/redbeat` to open the UI.

## Migrating internal code to open source
This repository provides a public-safe skeleton. When moving your internal blueprint:
- Replace imports like `common_utils.redis_utils`, `utils.celery.celery_app`, and custom loggers with public shims in this package (e.g., add `redis_client.py`, `celery_app.py`, `logging.py`).
- Avoid hardcoded company-specific settings or secrets; use environment variables.
- Add tests for critical operations (CRUD, import/export, batch reads).

## Development
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .[dev]
pre-commit install
pytest -q
```

## License
MIT
