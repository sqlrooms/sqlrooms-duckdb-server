uv run --group dev watchmedo auto-restart --pattern '*.py' --recursive --signal SIGTERM -- uv run python -m pkg --db-path /tmp/sqlrooms-duckdb-server-dev.spatial --port 30001

