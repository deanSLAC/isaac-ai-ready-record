"""
ISAAC AI-Ready Record - Flask REST API
Sidecar API for the Streamlit portal, providing programmatic access
to record validation and CRUD operations.

Endpoints are served under /portal/api/ to avoid conflict with
Authentik's /api path at the domain level.

Run standalone:  python portal/api.py
Run with gunicorn:  gunicorn -b 0.0.0.0:8502 portal.api:app
"""

import os
import sys
import json
import time
import logging
import functools
from pathlib import Path

import requests as http_requests
from flask import Flask, jsonify, request
from flask_cors import CORS
from jsonschema import Draft202012Validator

# ---------------------------------------------------------------------------
# Ensure the portal package directory is importable so we can do `import database`
# just like app.py does when Streamlit sets the CWD to portal/.
# ---------------------------------------------------------------------------
_portal_dir = Path(__file__).resolve().parent
if str(_portal_dir) not in sys.path:
    sys.path.insert(0, str(_portal_dir))

import database  # noqa: E402  (same import style as app.py)
import ontology  # noqa: E402

# ---------------------------------------------------------------------------
# Flask app setup
# ---------------------------------------------------------------------------
app = Flask(__name__)
CORS(app)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("isaac-portal-api")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PORT = int(os.environ.get("PORT", 8502))
AUTHENTIK_INTERNAL_URL = os.environ.get(
    "AUTHENTIK_INTERNAL_URL",
    "http://authentik-server.authentik.svc.cluster.local:9000",
)

# In-memory token cache: token -> {"user": str, "expires": float}
_token_cache: dict = {}
_TOKEN_CACHE_TTL = 300  # 5 minutes

# ---------------------------------------------------------------------------
# Load ISAAC record JSON Schema (Draft 2020-12)
# Schema lives at <project_root>/schema/isaac_record_v1.json
# api.py lives at <project_root>/portal/api.py  =>  go up one level
# ---------------------------------------------------------------------------
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schema" / "isaac_record_v1.json"
with open(SCHEMA_PATH) as f:
    ISAAC_SCHEMA = json.load(f)
ISAAC_VALIDATOR = Draft202012Validator(ISAAC_SCHEMA)

logger.info("Loaded ISAAC schema from %s", SCHEMA_PATH)


# ---------------------------------------------------------------------------
# Auth helper
# ---------------------------------------------------------------------------
def _validate_bearer_token(token: str) -> str | None:
    """
    Validate a Bearer token against Authentik.

    Calls GET /api/v3/core/users/me/ with the token.  Returns the username
    on success, or None if the token is invalid / Authentik is unreachable.
    Results are cached for 5 minutes to reduce load on Authentik.
    """
    now = time.monotonic()

    # Check cache
    cached = _token_cache.get(token)
    if cached and cached["expires"] > now:
        return cached["user"]

    # Evict expired entries (cheap linear scan — cache is small)
    expired_keys = [k for k, v in _token_cache.items() if v["expires"] <= now]
    for k in expired_keys:
        del _token_cache[k]

    try:
        resp = http_requests.get(
            f"{AUTHENTIK_INTERNAL_URL}/api/v3/core/users/me/",
            headers={"Authorization": f"Bearer {token}"},
            timeout=5,
        )
    except Exception as exc:
        logger.error("Authentik token validation request failed: %s", exc)
        return None

    if resp.status_code != 200:
        logger.info("Authentik rejected token (HTTP %d)", resp.status_code)
        return None

    try:
        username = resp.json()["user"]["username"]
    except (KeyError, TypeError, ValueError):
        logger.warning("Unexpected Authentik /users/me/ response: %s", resp.text[:200])
        return None

    _token_cache[token] = {"user": username, "expires": now + _TOKEN_CACHE_TTL}
    return username


def _get_auth_info():
    """
    Extract and validate authentication from the request.

    Supports two methods:
    1. Authentik SSO headers (X-authentik-username) — set by nginx forward-auth
       for browser sessions routed through the main portal ingress.
    2. Bearer token — validated against Authentik's /api/v3/core/users/me/.

    Returns a dict with 'method' and 'user', or None if unauthenticated.
    """
    # Check for Authentik SSO header (set by nginx auth_request)
    authentik_user = request.headers.get("X-authentik-username")
    if authentik_user:
        return {"method": "authentik_sso", "user": authentik_user}

    # Check for Bearer token
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
        username = _validate_bearer_token(token)
        if username:
            return {"method": "bearer_token", "user": username}
        # Token present but invalid — return None so _require_auth rejects it
        return None

    return None


def _log_request(auth_info):
    """Log incoming request with auth context."""
    if auth_info:
        logger.info(
            "%s %s [auth=%s user=%s]",
            request.method,
            request.path,
            auth_info.get("method"),
            auth_info.get("user"),
        )
    else:
        logger.info("%s %s [unauthenticated]", request.method, request.path)


def _require_auth(fn):
    """Decorator that enforces authentication on an endpoint."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        auth_info = _get_auth_info()
        _log_request(auth_info)
        if auth_info is None:
            return jsonify({
                "error": "authentication_required",
                "message": (
                    "Provide a valid Bearer token in the Authorization header. "
                    "Create one at https://isaac.slac.stanford.edu/auth/if/user/#/settings (Tokens and App passwords tab)"
                ),
            }), 401
        request.auth_info = auth_info
        return fn(*args, **kwargs)
    return wrapper


# ---------------------------------------------------------------------------
# Validation helper
# ---------------------------------------------------------------------------
def _validate_record(data: dict) -> list:
    """
    Validate a record dict against the ISAAC schema.
    Returns a list of error dicts; empty list means valid.
    Collects ALL errors (does not stop at first).
    """
    errors = []
    for err in ISAAC_VALIDATOR.iter_errors(data):
        errors.append({
            "path": "/".join(str(p) for p in err.absolute_path) or "(root)",
            "message": err.message,
        })
    return errors


def _validate_vocabulary(data: dict) -> list:
    """
    Validate a record dict against the live ontology vocabulary.
    Degrades gracefully: returns an empty list on any internal error.
    """
    try:
        return ontology.validate_record_vocabulary(data)
    except Exception as exc:
        logger.warning("Vocabulary validation failed (degraded): %s", exc)
        return []


# ===========================================================================
# Endpoints
# ===========================================================================

# --- Health check ----------------------------------------------------------

@app.route("/portal/api/health", methods=["GET"])
def health():
    """Health check for Kubernetes liveness/readiness probes."""
    return jsonify({"status": "healthy", "service": "isaac-portal-api"})


# --- Validate (dry-run, no DB write) --------------------------------------

@app.route("/portal/api/validate", methods=["POST"])
@_require_auth
def validate():
    """
    Validate a JSON body against the ISAAC record schema.
    Does NOT persist anything to the database.
    """

    data = request.get_json(silent=True)
    if data is None:
        return jsonify({
            "valid": False,
            "errors": [{"path": "(root)", "message": "Request body is not valid JSON"}],
        }), 400

    schema_errors = _validate_record(data)
    vocab_errors = _validate_vocabulary(data)
    all_errors = schema_errors + vocab_errors
    schema_valid = len(schema_errors) == 0
    vocab_valid = len(vocab_errors) == 0

    return jsonify({
        "valid": schema_valid and vocab_valid,
        "schema_valid": schema_valid,
        "vocabulary_valid": vocab_valid,
        "schema_errors": schema_errors,
        "vocabulary_errors": vocab_errors,
        "errors": all_errors,
    }), 200


# --- Create record ---------------------------------------------------------

@app.route("/portal/api/records", methods=["POST"])
@_require_auth
def create_record():
    """
    Validate and persist a new ISAAC record.
    """

    data = request.get_json(silent=True)
    if data is None:
        return jsonify({
            "success": False,
            "reason": "invalid_json",
            "message": "Request body is not valid JSON",
        }), 400

    # Schema validation
    schema_errors = _validate_record(data)
    vocab_errors = _validate_vocabulary(data)
    errors = schema_errors + vocab_errors
    if errors:
        return jsonify({
            "success": False,
            "reason": "validation_failed",
            "schema_errors": schema_errors,
            "vocabulary_errors": vocab_errors,
            "errors": errors,
        }), 400

    # Persist via shared database module
    try:
        record_id = database.save_record(data)
        return jsonify({"success": True, "record_id": record_id}), 201
    except ValueError as ve:
        # Missing required fields that passed schema but failed DB check
        return jsonify({
            "success": False,
            "reason": "validation_failed",
            "errors": [{"path": "(root)", "message": str(ve)}],
        }), 400
    except Exception as exc:
        logger.exception("Database error saving record")
        return jsonify({
            "success": False,
            "reason": "database_error",
            "message": str(exc),
        }), 500


# --- List records ----------------------------------------------------------

@app.route("/portal/api/records", methods=["GET"])
@_require_auth
def list_records():
    """
    List records (metadata only) with optional pagination.
    Query params: ?limit=100&offset=0
    """

    try:
        limit = int(request.args.get("limit", 100))
        offset = int(request.args.get("offset", 0))
    except (ValueError, TypeError):
        return jsonify({"error": "limit and offset must be integers"}), 400

    try:
        records = database.list_records(limit=limit, offset=offset)
        return jsonify(records), 200
    except Exception as exc:
        logger.exception("Database error listing records")
        return jsonify({"error": str(exc)}), 500


# --- Get single record -----------------------------------------------------

@app.route("/portal/api/records/<record_id>", methods=["GET"])
@_require_auth
def get_record(record_id):
    """
    Retrieve the full JSON for a single record by its ULID.
    """

    try:
        record = database.get_record(record_id)
    except Exception as exc:
        logger.exception("Database error fetching record %s", record_id)
        return jsonify({"error": str(exc)}), 500

    if record is None:
        return jsonify({"error": "Record not found"}), 404

    return jsonify(record), 200


# ===========================================================================
# Entrypoint
# ===========================================================================

if __name__ == "__main__":
    # Initialize database tables (same as the Streamlit app does on startup)
    if database.is_db_configured():
        logger.info("Initializing database tables...")
        database.init_tables()
    else:
        logger.warning(
            "Database not configured (PGHOST not set). "
            "Running without persistence -- DB endpoints will fail."
        )

    logger.info("Starting ISAAC Portal API on port %d", PORT)
    app.run(host="0.0.0.0", port=PORT, debug=os.environ.get("FLASK_DEBUG", "0") == "1")
