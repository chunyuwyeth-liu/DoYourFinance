## DoYourFinance

This project uses `uv` for Python tooling and dependency management.

### Quick start

```bash
uv sync
uv run python main.py
```

### Common tasks

```bash
uv add <package>
uv remove <package>
uv run python -m pytest
```

### Run the web app

```bash
uv run uvicorn app:app --reload
```
