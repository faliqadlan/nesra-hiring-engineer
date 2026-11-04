#!/bin/sh
# Start the service on http://localhost:8000
exec uv run uvicorn app.main:app --host 127.0.0.1 --port 8000 --no-access-log "$@"
