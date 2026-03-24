# Vonnegut — Data Explorer & Migration Platform
# Install just: brew install just
# Run `just` to see all available commands.

set dotenv-load

# Default: list available commands
default:
    @just --list

# ── Setup ──────────────────────────────────────────────

# Install all dependencies (backend + frontend)
install:
    cd backend && uv sync
    cd frontend && npm install

# Install backend dependencies only
install-backend:
    cd backend && uv sync

# Install frontend dependencies only
install-frontend:
    cd frontend && npm install

# ── Development ────────────────────────────────────────

# Run backend + frontend dev servers concurrently
dev:
    #!/usr/bin/env bash
    trap 'kill 0' EXIT
    just dev-backend &
    just dev-frontend &
    wait

# Run backend dev server (port 8000)
dev-backend:
    cd backend && uv run uvicorn vonnegut.main:app --reload --host 0.0.0.0 --port 8000 --factory

# Run frontend dev server (port 5173, proxies /api to backend)
dev-frontend:
    cd frontend && npm run dev

# ── Testing ────────────────────────────────────────────

# Run all backend tests
test *args='':
    cd backend && uv run pytest {{ args }}

# Run backend tests in verbose mode
test-v *args='':
    cd backend && uv run pytest -v {{ args }}

# Run backend tests with coverage (install pytest-cov first)
test-cov:
    cd backend && uv run pytest --cov=vonnegut --cov-report=term-missing

# Run a specific test file or pattern
test-match pattern:
    cd backend && uv run pytest -v -k "{{ pattern }}"

# ── Build ──────────────────────────────────────────────

# Build frontend for production
build:
    cd frontend && npm run build

# Type-check frontend without emitting
typecheck:
    cd frontend && npx tsc --noEmit

# Lint frontend
lint:
    cd frontend && npm run lint

# ── Database ───────────────────────────────────────────

# Reset the local SQLite database
[confirm("This will delete vonnegut.db. Continue?")]
db-reset:
    rm -f backend/vonnegut.db

# ── Utilities ──────────────────────────────────────────

# Run any backend command via uv
uv *args:
    cd backend && uv run {{ args }}

# Open the app in the default browser
open:
    open http://localhost:5173

# Check that everything works (tests + build)
check:
    just test-v
    just typecheck
    just build
    @echo "All checks passed."

# Clean build artifacts and caches
clean:
    rm -rf frontend/dist
    rm -rf backend/.pytest_cache
    find backend -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
