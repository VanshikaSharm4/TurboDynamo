#!/bin/bash
# ============================================================================
# Kasoku CLI Helper - Start server + use kvctl like Redis CLI
# 
# Usage:
#   ./kvctl.sh                         Start server, open interactive shell
#   ./kvctl.sh put user:1 "Alice"     Run a single command (auto-starts server)
#   ./kvctl.sh get user:1
#   ./kvctl.sh keys
#   ./kvctl.sh scan user:
#   ./kvctl.sh delete user:1
#   ./kvctl.sh shell
#   ./kvctl.sh --addr http://192.168.1.5:9000 get user:1   # remote server
#
# First run:
#   chmod +x kvctl.sh
#   ./kvctl.sh put user:1 "Alice"
# ============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

BOLD='\033[1m'
GREEN='\033[32m'
CYAN='\033[36m'
YELLOW='\033[33m'
RED='\033[31m'
DIM='\033[2m'
RESET='\033[0m'

SERVER_PORT=9000
SERVER_PID=""

cleanup() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        echo -e "\n${DIM}Stopping server...${RESET}"
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

# ── Check for --addr flag ──────────────────────────────────────────────────
SERVER_ADDR=""
ARGS=()
for arg in "$@"; do
    if [ "$arg" = "--addr" ]; then
        SERVER_ADDR="__NEXT__"
    elif [ "$SERVER_ADDR" = "__NEXT__" ]; then
        SERVER_ADDR="$arg"
    else
        ARGS+=("$arg")
    fi
done

if [ -n "$SERVER_ADDR" ] && [ "$SERVER_ADDR" != "__NEXT__" ]; then
    # Remote mode — use existing server, don't start one
    if ! command -v kvctl &>/dev/null && [ ! -f ./kvctl ]; then
        echo -e "${YELLOW}Building kvctl...${RESET}"
        go build -o kvctl ./cmd/kvctl/
    fi
    KVCTL="./kvctl"
    if command -v kvctl &>/dev/null; then
        KVCTL="kvctl"
    fi
    echo -e "${DIM}kvctl --addr $SERVER_ADDR ${ARGS[*]}${RESET}"
    exec $KVCTL --addr "$SERVER_ADDR" "${ARGS[@]}"
fi

# ── Local mode ─────────────────────────────────────────────────────────────
# Build if needed
if [ ! -f ./kasoku-server ]; then
    echo -e "${YELLOW}Building server...${RESET}"
    go build -o kasoku-server ./cmd/server/
fi
if [ ! -f ./kvctl ] && ! command -v kvctl &>/dev/null; then
    echo -e "${YELLOW}Building kvctl...${RESET}"
    go build -o kvctl ./cmd/kvctl/
fi

KVCTL="./kvctl"
if command -v kvctl &>/dev/null; then
    KVCTL="kvctl"
fi

# Check if server is already running
if curl -sf "http://localhost:$SERVER_PORT/health" >/dev/null 2>&1; then
    echo -e "${GREEN}✓${RESET} Server already running on :$SERVER_PORT"
else
    echo -e "${YELLOW}Starting server on :$SERVER_PORT...${RESET}"
    ./kasoku-server --config configs/example.yaml &>/tmp/kasoku-server.log &
    SERVER_PID=$!
    sleep 2
    if ! curl -sf "http://localhost:$SERVER_PORT/health" >/dev/null 2>&1; then
        sleep 2
    fi
    if curl -sf "http://localhost:$SERVER_PORT/health" >/dev/null 2>&1; then
        echo -e "${GREEN}✓${RESET} Server running on :$SERVER_PORT"
    else
        echo -e "${RED}✗${RESET} Server failed to start. Check /tmp/kasoku-server.log"
        exit 1
    fi
fi

if [ ${#ARGS[@]} -eq 0 ]; then
    ARGS=("shell")
fi

echo -e "${DIM}kvctl --addr http://localhost:$SERVER_PORT ${ARGS[*]}${RESET}"
exec $KVCTL --addr "http://localhost:$SERVER_PORT" "${ARGS[@]}"
