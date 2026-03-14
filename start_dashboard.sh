#!/usr/bin/env bash
# Dashboard startup script

# If invoked as `sh start_dashboard.sh`, re-exec with bash.
# Must be POSIX-sh compatible so `sh` can reach the exec.
if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

echo "========================================"
echo "Order Analysis Dashboard Installer"
echo "========================================"
echo ""

# Find a Python 3 interpreter
PY_BIN=""
if command -v python3 >/dev/null 2>&1; then
    PY_BIN="python3"
elif command -v python >/dev/null 2>&1; then
    PY_BIN="python"
else
    echo "Error: Python is not installed (need Python 3)"
    exit 1
fi

PY_MAJOR=$($PY_BIN -c 'import sys; print(sys.version_info[0])' 2>/dev/null || echo "")
if [[ "$PY_MAJOR" != "3" ]]; then
    echo "Error: $PY_BIN is not Python 3 (found: $($PY_BIN --version 2>/dev/null || echo unknown))"
    echo "Install Python 3 and ensure python3 is on PATH."
    exit 1
fi

echo "✓ Python found: $($PY_BIN --version)"
echo ""

# Check if pip is available for this interpreter
if ! $PY_BIN -m pip --version >/dev/null 2>&1; then
    echo "pip is not available for $PY_BIN; attempting to bootstrap via ensurepip…"
    if $PY_BIN -m ensurepip --upgrade >/dev/null 2>&1; then
        :
    else
        echo "Error: pip is not available for $PY_BIN and ensurepip failed."
        echo "On Ubuntu, try: sudo apt-get install python3-pip python3-venv"
        echo "Or if you use conda: conda install -n base pip"
        exit 1
    fi
fi

echo "✓ pip found: $($PY_BIN -m pip --version | awk '{print $1" "$2" "$3}')"
echo ""

# Prefer a local venv to avoid conda/native-extension build issues (e.g. qpython)
VENV_DIR="${DASHBOARD_VENV_DIR:-.venv}"

if [[ ! -x "$VENV_DIR/bin/python" ]]; then
    echo "Creating virtual environment at: $VENV_DIR"
    $PY_BIN -m venv "$VENV_DIR" || {
        echo "Error: failed to create venv ($PY_BIN -m venv)"
        echo "If you're on Ubuntu, you may need: sudo apt-get install python3-venv"
        exit 1
    }
fi

echo "Using venv python: $VENV_DIR/bin/python"

# Install requirements
echo "Installing dependencies (in venv)..."
"$VENV_DIR/bin/python" -m pip install --upgrade pip setuptools wheel
"$VENV_DIR/bin/python" -m pip install -r requirements.txt

echo ""
echo "✓ Installation complete!"
echo ""
echo "========================================"
echo "Starting Dashboard..."
echo "========================================"
echo ""

INSTALL_ONLY=0
ERR_FILE=""
PORT_ARG=""
DASHBOARD_ARGS=()

# Basic arg parsing so flags like --port/--err-file work.
# Backwards compat: a single positional arg is treated as an err file path.
while [[ $# -gt 0 ]]; do
    case "$1" in
        --install-only)
            INSTALL_ONLY=1
            shift
            ;;
        --err-file)
            ERR_FILE="${2:-}"
            shift 2
            ;;
        --port)
            PORT_ARG="${2:-}"
            DASHBOARD_ARGS+=("--port" "$PORT_ARG")
            shift 2
            ;;
        *)
            if [[ -z "$ERR_FILE" && "$1" != -* ]]; then
                ERR_FILE="$1"
                shift
            else
                DASHBOARD_ARGS+=("$1")
                shift
            fi
            ;;
    esac
done

if [[ "$INSTALL_ONLY" == "1" ]]; then
    echo "Install-only requested; exiting after dependency install."
    exit 0
fi

PORT_DISPLAY="${PORT_ARG:-${DASHBOARD_PORT:-5050}}"

echo "Access the dashboard at: http://localhost:${PORT_DISPLAY}"
echo ""
echo "Endpoints:"
echo "  GET  /                    - Main dashboard"
echo "  GET  /force-orders         - Force orders page"
echo "  GET  /api/force-orders     - Force orders JSON (login required)"
echo "  GET  /health               - Health check"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

# Start dashboard
if [[ -n "$ERR_FILE" ]]; then
    echo "Using err file: $ERR_FILE"
    "$VENV_DIR/bin/python" dashboard.py --err-file "$ERR_FILE" "${DASHBOARD_ARGS[@]}"
else
    if [[ -n "${MUSE2_ERR_FILE:-}" ]]; then
        echo "Using err file (from MUSE2_ERR_FILE): $MUSE2_ERR_FILE"
    fi
    "$VENV_DIR/bin/python" dashboard.py "${DASHBOARD_ARGS[@]}"
fi
