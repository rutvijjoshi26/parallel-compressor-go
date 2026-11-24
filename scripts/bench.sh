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
# between runs. Every round trip is checked with cmp.
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

# timed OUT IN CMD... runs CMD <IN >OUT once and prints wall seconds.
timed() {
	local out=$1 in=$2
	shift 2
	local t0=$EPOCHREALTIME
	"$@" <"$in" >"$out"
	local t1=$EPOCHREALTIME
	awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.6f\n", b - a }'
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

# measure DATASET LABEL EXT "COMPRESS CMD" "DECOMPRESS CMD"|""
# A config without its own timed decompression is still decompressed once
# (untimed) with pczip/gzip/zstd -d and checked with cmp.
measure() {
	local ds=$1 label=$2 ext=$3 ccmd=$4 dcmd=$5 verify=$6
	local in=$BENCH_DIR/$ds.bin out=$BENCH_DIR/out.$ext back=$BENCH_DIR/back.bin
	local n ct=() dt=() i
	n=$(stat -c %s "$in")
	for ((i = 0; i < BENCH_RUNS; i++)); do
		rm -f "$out"
		# shellcheck disable=SC2086
		ct+=("$(timed "$out" "$in" $ccmd)")
	done
	local csize
	csize=$(stat -c %s "$out")
	if [[ -n $dcmd ]]; then
		for ((i = 0; i < BENCH_RUNS; i++)); do
			rm -f "$back"
			# shellcheck disable=SC2086
			dt+=("$(timed "$back" "$out" $dcmd)")
		done
	else
		rm -f "$back"
		# shellcheck disable=SC2086
		$verify <"$out" >"$back"
	fi
	cmp "$in" "$back" || { echo "ROUND TRIP FAILED: $ds $label" >&2; exit 1; }
	if [[ $ext == pcz ]]; then
		# Compressed output must be byte-identical for every pczip config.
		if [[ -f $BENCH_DIR/ref.pcz ]]; then
			cmp "$out" "$BENCH_DIR/ref.pcz" || { echo "pczip output differs: $ds $label" >&2; exit 1; }
		else
			cp "$out" "$BENCH_DIR/ref.pcz"
		fi
	fi
	printf '%s\t%s\tcompress\t%s\n' "$ds" "$label" "${ct[*]}" >>"$RAW"
	local dcell="–"
	if [[ -n $dcmd ]]; then
		printf '%s\t%s\tdecompress\t%s\n' "$ds" "$label" "${dt[*]}" >>"$RAW"
		dcell=$(cell "$n" "$(stats "${dt[@]}")")
	fi
	local ratio
	ratio=$(awk -v a="$n" -v b="$csize" 'BEGIN { printf "%.3f", a / b }')
	ROWS+=("| $ds | $label | $ratio | $(cell "$n" "$(stats "${ct[@]}")") | $dcell |")
	rm -f "$out" "$back"
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
	rm -f "$BENCH_DIR/ref.pcz"

	for sched in static steal; do
		for w in "${wlist[@]}"; do
			d=""
			[[ $sched == steal && $w == "$MAXW" ]] && d="$PCZIP -d -workers $w -sched steal - -"
			measure "$ds" "pczip -sched $sched -workers $(wlabel "$w")" pcz \
				"$PCZIP -workers $w -sched $sched - -" "$d" "$PCZIP -d - -"
		done
	done
	if have gzip; then
		measure "$ds" "gzip -1" gz "gzip -1 -c" "gzip -d -c" ""
		measure "$ds" "gzip -6" gz "gzip -6 -c" "gzip -d -c" ""
	else
		echo "gzip not installed; skipping" >&2
	fi
	if have zstd; then
		measure "$ds" "zstd -1 -T0" zst "zstd -q -1 -T0 -c" "zstd -q -d -c" ""
		measure "$ds" "zstd -1 -T1" zst "zstd -q -1 -T1 -c" "zstd -q -d -c" ""
	else
		echo "zstd not installed; skipping" >&2
	fi
	rm -f "$BENCH_DIR/ref.pcz"
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
