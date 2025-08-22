# Contributing

Thanks for your interest in contributing!

## Development setup
- Python 3.9+
- Create a virtualenv and install this package in editable mode:
  
  ```bash
  python -m venv .venv && source .venv/bin/activate
  python -m pip install --upgrade pip
  pip install -e .[dev]
  pre-commit install
  ```

## Making changes
- Create a branch from `main`
- Add tests for changes where applicable
- Run formatting and linters: `pre-commit run -a`
- Ensure `pytest` passes

## Pull Requests
- Describe the change and motivation
- Include screenshots for UI changes
- Link related issues

## Release process
- Update `CHANGELOG.md`
- Bump version in `pyproject.toml`
- Create a GitHub release with tags