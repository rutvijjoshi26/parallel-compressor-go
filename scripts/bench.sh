#!/usr/bin/env bash
# Reproducible benchmark of pczip against gzip and zstd.
#
#   bash scripts/bench.sh
#
# Environment:
#   BENCH_DIR   where datasets and outputs go (default: $HOME/bench-data).
#               Use a disk directory with about 2 GB free; the script does
#               not delete it, so remove it when done.
#   BENCH_SIZE  size of each dataset (default: 256MiB).
#   BENCH_RUNS  timed runs per measurement (default: 3); the median, min
#               and max are reported.
#
# Every tool reads the input file on stdin and writes stdout to a file in
# BENCH_DIR, so all of them do the same I/O (pczip with "-" "-" does not
# fsync; neither do gzip and zstd here). Inputs are read once before timing
# so every run starts with them in the page cache. Outputs are deleted
# between runs. Every timed run's result is checked with cmp, and the script
# stops at the first failing tool or mismatch.
#
# MB/s = (input bytes / 1e6) / wall seconds, for both compression and
# decompression (input = the uncompressed dataset). Ratio = input bytes /
# compressed bytes.
set -euo pipefail

BENCH_DIR=${BENCH_DIR:-$HOME/bench-data}
BENCH_SIZE=${BENCH_SIZE:-256MiB}
BENCH_RUNS=${BENCH_RUNS:-3}
REPO=$(cd "$(dirname "$0")/.." && pwd)
MAXW=$(getconf _NPROCESSORS_ONLN)

mkdir -p "$BENCH_DIR/bin"
echo "building pczip and gendata into $BENCH_DIR/bin" >&2
(cd "$REPO" && go build -o "$BENCH_DIR/bin/pczip" ./cmd/pczip && go build -o "$BENCH_DIR/bin/gendata" ./cmd/gendata)
PCZIP=$BENCH_DIR/bin/pczip

have() { command -v "$1" >/dev/null 2>&1; }

# Scratch files for one measurement; removed on any exit.
OUT=$BENCH_DIR/out.bin
BACK=$BENCH_DIR/back.bin
REF=$BENCH_DIR/ref.pcz
trap 'rm -f "$OUT" "$BACK" "$REF"' EXIT

# timed OUT IN CMD... runs CMD <IN >OUT once and prints wall seconds. It
# returns non-zero if CMD fails; callers check it with
# v=$(timed ...) || fail ..., so a failing tool never yields a time.
timed() {
	local out=$1 in=$2
	shift 2
	local t0=$EPOCHREALTIME
	"$@" <"$in" >"$out" || return 1
	local t1=$EPOCHREALTIME
	awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.6f\n", b - a }'
}

fail() {
	echo "FAILED: $*" >&2
	exit 1
}

# stats prints "median min max" of its numeric arguments.
stats() {
	printf '%s\n' "$@" | sort -g | awk '{ v[NR] = $1 } END {
		m = (NR % 2) ? v[(NR + 1) / 2] : (v[NR / 2] + v[NR / 2 + 1]) / 2
		printf "%s %s %s\n", m, v[1], v[NR] }'
}

# mbps BYTES SECONDS
mbps() { awk -v n="$1" -v s="$2" 'BEGIN { printf "%.0f", n / 1e6 / s }'; }

# cell BYTES "median min max" -> "MB/s (min–max)"; faster is the min time.
cell() {
	local n=$1 med lo hi
	read -r med lo hi <<<"$2"
	echo "$(mbps "$n" "$med") ($(mbps "$n" "$hi")–$(mbps "$n" "$lo"))"
}

RAW=$BENCH_DIR/raw-times.tsv
printf 'dataset\tconfig\tphase\tseconds...\n' >"$RAW"
ROWS=()

# measure DATASET LABEL PCZ
#
# The commands come from three global arrays, set by the caller:
#   CCMD  compresses stdin to stdout (timed, BENCH_RUNS times);
#   DCMD  decompresses stdin to stdout, timed BENCH_RUNS times, or empty
#         for no timed decompression;
#   VCMD  decompresses stdin to stdout, untimed, to check each compress run.
# After every timed compress run, the output is decompressed with VCMD and
# compared with cmp; after every timed decompress run, the output is
# compared with cmp. PCZ=1 also checks that the compressed bytes are
# identical to the first pczip config's for this dataset.
measure() {
	local ds=$1 label=$2 pcz=$3
	local in=$BENCH_DIR/$ds.bin n ct=() dt=() i t csize
	n=$(stat -c %s "$in")
	for ((i = 0; i < BENCH_RUNS; i++)); do
		rm -f "$OUT" "$BACK"
		t=$(timed "$OUT" "$in" "${CCMD[@]}") || fail "$ds $label: compress run $((i + 1)) exited non-zero"
		ct+=("$t")
		"${VCMD[@]}" <"$OUT" >"$BACK" || fail "$ds $label: decompress for check"
		cmp "$in" "$BACK" || fail "$ds $label: round trip after compress run $((i + 1))"
		if ((pcz)); then
			if [[ -f $REF ]]; then
				cmp "$OUT" "$REF" || fail "$ds $label: pczip output differs between configs"
			else
				cp "$OUT" "$REF"
			fi
		fi
	done
	csize=$(stat -c %s "$OUT")
	if ((${#DCMD[@]})); then
		for ((i = 0; i < BENCH_RUNS; i++)); do
			rm -f "$BACK"
			t=$(timed "$BACK" "$OUT" "${DCMD[@]}") || fail "$ds $label: decompress run $((i + 1)) exited non-zero"
			dt+=("$t")
			cmp "$in" "$BACK" || fail "$ds $label: round trip after decompress run $((i + 1))"
		done
	fi
	printf '%s\t%s\tcompress\t%s\n' "$ds" "$label" "${ct[*]}" >>"$RAW"
	local dcell="–"
	if ((${#DCMD[@]})); then
		printf '%s\t%s\tdecompress\t%s\n' "$ds" "$label" "${dt[*]}" >>"$RAW"
		dcell=$(cell "$n" "$(stats "${dt[@]}")")
	fi
	local ratio
	ratio=$(awk -v a="$n" -v b="$csize" 'BEGIN { printf "%.3f", a / b }')
	ROWS+=("| $ds | $label | $ratio | $(cell "$n" "$(stats "${ct[@]}")") | $dcell |")
	rm -f "$OUT" "$BACK"
	echo "done: $ds $label" >&2
}

wlist=(1 2)
((MAXW > 2)) && wlist+=("$MAXW")
wlabel() { if [[ $1 == "$MAXW" ]]; then echo "$1 (max)"; else echo "$1"; fi; }

for ds in text logs mixed; do
	in=$BENCH_DIR/$ds.bin
	echo "generating $ds ($BENCH_SIZE, seed 1)" >&2
	"$BENCH_DIR/bin/gendata" -kind "$ds" -size "$BENCH_SIZE" -out "$in"
	cat "$in" >/dev/null # warm the page cache
	rm -f "$REF"

	for sched in static steal; do
		for w in "${wlist[@]}"; do
			CCMD=("$PCZIP" -workers "$w" -sched "$sched" - -)
			VCMD=("$PCZIP" -d - -)
			DCMD=()
			# Timed decompression on 1 worker (like-for-like with the
			# single-threaded gzip -d and zstd -d) and on all cores.
			if [[ $sched == steal && ($w == 1 || $w == "$MAXW") ]]; then
				DCMD=("$PCZIP" -d -workers "$w" -sched steal - -)
			fi
			measure "$ds" "pczip -sched $sched -workers $(wlabel "$w")" 1
		done
	done
	if have gzip; then
		VCMD=(gzip -d -c)
		DCMD=(gzip -d -c)
		CCMD=(gzip -1 -c)
		measure "$ds" "gzip -1" 0
		CCMD=(gzip -6 -c)
		measure "$ds" "gzip -6" 0
	else
		echo "gzip not installed; skipping" >&2
	fi
	if have zstd; then
		VCMD=(zstd -q -d -c)
		DCMD=(zstd -q -d -c)
		CCMD=(zstd -q -1 -T0 -c)
		measure "$ds" "zstd -1 -T0" 0
		CCMD=(zstd -q -1 -T1 -c)
		measure "$ds" "zstd -1 -T1" 0
	else
		echo "zstd not installed; skipping" >&2
	fi
	rm -f "$REF"
done

echo
echo "Dataset size: $BENCH_SIZE each. Runs per cell: $BENCH_RUNS. MB/s cells: median (min–max)."
echo
echo "| dataset | tool/config | ratio | compress MB/s | decompress MB/s |"
echo "|---|---|---|---|---|"
printf '%s\n' "${ROWS[@]}"
echo
echo "Raw wall times (seconds) are in $RAW:"
echo
cat "$RAW"
