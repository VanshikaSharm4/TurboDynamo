#!/bin/bash
# ============================================================================
# Kasoku YCSB Benchmark Runner
# Builds, starts 3-node cluster, runs workloads, reports results
#
# Usage:
#   ./bench.sh                          # Workload B (read mostly)
#   ./bench.sh a                        # Workload A (50/50)
#   ./bench.sh b                        # Workload B (95/5 read)
#   ./bench.sh c                        # Workload C (100% read)
#   ./bench.sh all                      # All workloads A,B,C,D,F
#   ./bench.sh a --records 50000 --dur 5 --workers 20
#   ./bench.sh --single                 # Single-node only
#   ./bench.sh --help
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

RECORDS=100000
WORKERS=50
BATCH=1
FIELDLEN=100
DURATION=10
MODE="cluster"
WORKLOADS="b"

usage() {
    echo -e "${BOLD}Usage:${RESET} $0 [workload] [options]"
    echo ""
    echo "Workloads:"
    echo "  a          Update heavy (50% read, 50% write)"
    echo "  b          Read mostly (95% read, 5% write) [default]"
    echo "  c          Read only (100% read)"
    echo "  d          Read latest (95% read, 5% insert)"
    echo "  f          Read-modify-write (50% read, 50% RMW)"
    echo "  all        All of the above"
    echo ""
    echo "Options:"
    echo "  --single        Run single-node instead of cluster"
    echo "  --records N     Number of records (default: $RECORDS)"
    echo "  --workers N     Worker count (default: $WORKERS)"
    echo "  --dur N         Test duration in seconds (default: $DURATION)"
    echo "  --batch N       Batch size (default: $BATCH)"
    echo "  --fieldlen N    Value size in bytes (default: $FIELDLEN)"
    echo "  --help          Show this help"
    echo ""
    echo -e "${DIM}Examples:${RESET}"
    echo "  $0 a                 # Workload A, 3-node cluster"
    echo "  $0 c --single --dur 5  # Workload C, single node, 5s"
    echo "  $0 all --records 50000 --workers 20"
    exit 0
}

# Parse args
ARGS=()
for arg in "$@"; do
    case "$arg" in
        --help) usage ;;
        --single) MODE="single" ;;
        --records=*) RECORDS="${arg#*=}" ;;
        --workers=*) WORKERS="${arg#*=}" ;;
        --dur=*) DURATION="${arg#*=}" ;;
        --batch=*) BATCH="${arg#*=}" ;;
        --fieldlen=*) FIELDLEN="${arg#*=}" ;;
        all) WORKLOADS="a b c d f" ;;
        a|b|c|d|f) WORKLOADS="$arg" ;;
        --records|--workers|--dur|--batch|--fieldlen) ;;  # handled in next iteration
        *) ARGS+=("$arg") ;;
    esac
done

# Parse option values
PARSED_ARGS=()
skip_next=false
for i in $(seq 0 $(($# - 1))); do
    if $skip_next; then skip_next=false; continue; fi
    arg="${!i}"
    case "$arg" in
        --records) j=$((i+1)); RECORDS="${!j}"; skip_next=true ;;
        --workers) j=$((i+1)); WORKERS="${!j}"; skip_next=true ;;
        --dur) j=$((i+1)); DURATION="${!j}"; skip_next=true ;;
        --batch) j=$((i+1)); BATCH="${!j}"; skip_next=true ;;
        --fieldlen) j=$((i+1)); FIELDLEN="${!j}"; skip_next=true ;;
    esac
done

echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}${CYAN}║  Kasoku YCSB Benchmark${RESET}"
echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════════════════════╝${RESET}"
echo ""
echo -e "  Mode:       ${BOLD}$MODE${RESET}"
echo -e "  Records:    ${BOLD}$RECORDS${RESET}"
echo -e "  Workers:    ${BOLD}$WORKERS${RESET}"
echo -e "  Duration:   ${BOLD}${DURATION}s${RESET}"
echo -e "  Batch:      ${BOLD}$BATCH${RESET}"
echo -e "  Field Len:  ${BOLD}${FIELDLEN}B${RESET}"
echo -e "  Workloads:  ${BOLD}$WORKLOADS${RESET}"
echo ""

# Build
echo -e "${YELLOW}Building...${RESET}"
go build -o kasoku-server ./cmd/server/
go build -o ycsb-bench ./cmd/ycsb/
echo -e "${GREEN}✓${RESET} Built"
echo ""

cleanup() {
    echo -e "${DIM}Cleaning up...${RESET}"
    pkill -f "kasoku-server" 2>/dev/null || true
    sleep 1
}
trap cleanup EXIT INT TERM

run_workload() {
    local wl="$1"
    echo -e "${BOLD}━━━ Workload $wl ━━━${RESET}"

    if [ "$MODE" = "cluster" ]; then
        pkill -f "kasoku-server" 2>/dev/null || true
        sleep 1
        rm -rf data/bench-node1 data/bench-node2 data/bench-node3

        ./kasoku-server --config configs/bench-cluster-node1.yaml &>/dev/null &
        ./kasoku-server --config configs/bench-cluster-node2.yaml &>/dev/null &
        ./kasoku-server --config configs/bench-cluster-node3.yaml &>/dev/null &
        sleep 3

        for port in 9001 9011 9021; do
            if ! curl -sf "http://localhost:$port/health" >/dev/null 2>&1; then
                sleep 2
                if ! curl -sf "http://localhost:$port/health" >/dev/null 2>&1; then
                    echo -e "${RED}✗ Node :$port failed${RESET}"
                    return
                fi
            fi
        done

        ./ycsb-bench \
            -nodes "localhost:9100,localhost:9101,localhost:9102" \
            -workers "$WORKERS" \
            -batch "$BATCH" \
            -recordcount "$RECORDS" \
            -fieldlength "$FIELDLEN" \
            -dur "$DURATION" \
            -workload "$wl"
    else
        pkill -f "kasoku-server" 2>/dev/null || true
        sleep 1
        rm -rf data/ycsb-single
        mkdir -p data/ycsb-single

        KASOKU_DATA_DIR=./data/ycsb-single KASOKU_CONFIG=./configs/bench-realistic.yaml \
        ./kasoku-server &>/dev/null &
        sleep 2

        if ! curl -sf "http://localhost:9001/health" >/dev/null 2>&1; then
            sleep 2
        fi

        ./ycsb-bench \
            -nodes "localhost:9100" \
            -workers "$WORKERS" \
            -batch "$BATCH" \
            -recordcount "$RECORDS" \
            -fieldlength "$FIELDLEN" \
            -dur "$DURATION" \
            -workload "$wl"
    fi
    echo ""
}

for wl in $WORKLOADS; do
    run_workload "$wl"
done

echo -e "${BOLD}${GREEN}Done.${RESET}"
