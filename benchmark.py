# file: benchmark_speedup_fragmented_and_realworld.py
import os
import glob
import subprocess
import time
import hashlib
import random
from typing import Dict, List, Tuple, Callable
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ---- Config ----
GO_EXE = "pczip"
if os.name == "nt":
    GO_EXE += ".exe"

FILE_SIZE_MB = 512
THREAD_COUNTS: List[int] = [2, 4, 6, 8, 12]
ITERATIONS = 3

# (flag used by CLI, human-readable label)
PARALLEL_IMPLS: List[Tuple[str, str]] = [
    ("bsp", "BSP (Static)"),
    ("ws",  "Work Stealing"),
]

# Two datasets: fragmented (your real-world pick) + mixed real-worldish
DATASETS: List[Tuple[str, str]] = [
    ("fragmented",      "Fragmented (20% rand | 60% zeros | 20% rand)"),
    ("realworld_mixed", "Mixed: text+JSON+zeros+media-like+random"),
]

# ---- Helpers ----
def _exe_path() -> str:
    return GO_EXE if os.name == "nt" else f"./{GO_EXE}"

def _dataset_path(dataset_key: str) -> str:
    return f"{dataset_key}.bin"

def compile_go() -> None:
    try:
        subprocess.run(["go", "build", "-o", GO_EXE, "main.go"], check=True)
    except subprocess.CalledProcessError as exc:
        raise SystemExit(1)

def _md5sum(fname: str) -> str:
    h = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

# ---- Dataset generators ----
def gen_fragmented(path: str, size_mb: int) -> None:
    """why: mimics sparse disk regions with random 'islands'."""
    if os.path.exists(path):
        os.remove(path)
    total_size = size_mb * 1024 * 1024
    island_size = int((total_size * 0.20) / 2)
    gap_size = total_size - (island_size * 2)
    with open(path, "wb") as f:
        f.write(os.urandom(island_size))
        chunk = 10 * 1024 * 1024
        zero_block = b"\x00" * chunk
        written = 0
        while written < gap_size:
            towrite = min(chunk, gap_size - written)
            f.write(zero_block[:towrite])
            written += towrite
        f.write(os.urandom(island_size))

def _write_repeat(f, pattern: bytes, length: int) -> None:
    """why: avoid huge allocations for long runs."""
    buf = pattern
    remaining = length
    while remaining > 0:
        w = min(len(buf), remaining)
        f.write(buf[:w])
        remaining -= w

def _make_text_block(block_bytes: int) -> bytes:
    line = (b"The quick brown fox jumps over the lazy dog. "
            b"lorem ipsum dolor sit amet, consectetur adipiscing elit. ")
    out = bytearray()
    while len(out) + len(line) + 1 <= block_bytes:
        out.extend(line)
        out.append(0x0A)
    if len(out) < block_bytes:
        out.extend(line[: block_bytes - len(out)])
    return bytes(out)

def _make_jsonlog_block(block_bytes: int) -> bytes:
    # Structured, repetitive keys with mild variance.
    out = bytearray()
    i = 0
    while len(out) < block_bytes:
        lvl = random.choice(("INFO", "WARN", "ERROR", "DEBUG"))
        uid = random.randint(1, 10000)
        msg = random.choice((
            "connected", "disconnected", "timeout", "retrying", "ok",
            "downloaded", "uploaded", "cached", "evicted", "committed"
        ))
        line = (f'{{"ts":{1700000000+i},"level":"{lvl}","user":{uid},'
                f'"msg":"{msg}","ok":true}}\n').encode("utf-8")
        if len(out) + len(line) > block_bytes:
            out.extend(line[: block_bytes - len(out)])
            break
        out.extend(line)
        i += 1
    return bytes(out)

def _make_media_like_block(block_bytes: int) -> bytes:
    # Header-like repetition + noisy body.
    header = b"\x89MEDIAHDR" + os.urandom(56)  # fixed-ish header region
    header = (header * ((1024 // len(header)) + 1))[:1024]
    noisy = os.urandom(max(0, block_bytes - len(header)))
    return header + noisy

def gen_realworld_mixed(path: str, size_mb: int) -> None:
    """why: mixes compressible and incompressible regions like real corpora."""
    if os.path.exists(path):
        os.remove(path)
    random.seed(42)  # reproducible layout
    total = size_mb * 1024 * 1024
    block = 4 * 1024 * 1024  # 4 MiB blocks
    kinds = ("zeros", "text", "json", "media", "random")
    # Rough proportions: zeros(25), text(20), json(20), media(20), random(15)
    weights = (25, 20, 20, 20, 15)
    with open(path, "wb") as f:
        written = 0
        while written < total:
            bsz = min(block, total - written)
            kind = random.choices(kinds, weights=weights, k=1)[0]
            if kind == "zeros":
                _write_repeat(f, b"\x00" * min(1_048_576, bsz), bsz)
            elif kind == "text":
                f.write(_make_text_block(bsz))
            elif kind == "json":
                f.write(_make_jsonlog_block(bsz))
            elif kind == "media":
                f.write(_make_media_like_block(bsz))
            else:  # random
                # why: incompressible areas (e.g., encrypted/media tail)
                chunk = 10 * 1024 * 1024
                togo = bsz
                while togo > 0:
                    w = min(chunk, togo)
                    f.write(os.urandom(w))
                    togo -= w
            written += bsz

GENERATOR_BY_KEY: Dict[str, Callable[[str, int], None]] = {
    "fragmented": gen_fragmented,
    "realworld_mixed": gen_realworld_mixed,
}

# ---- Benchmarking ----
def run_compress(in_file: str, impl: str, threads: int, output_file: str) -> Tuple[float, float]:
    """Return (duration_seconds, compression_ratio). Keep artifact; cleaned later."""
    if os.path.exists(output_file):
        os.remove(output_file)
    start = time.time()
    cmd = [
        _exe_path(), "-mode", "compress", "-in", in_file,
        "-out", output_file, "-impl", impl, "-threads", str(threads),
    ]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError as exc:
        raise
    duration = time.time() - start
    ratio = 0.0
    if os.path.exists(output_file):
        orig_size = os.path.getsize(in_file)
        comp_size = os.path.getsize(output_file)
        if comp_size > 0:
            ratio = orig_size / comp_size  # why: catch anomalies
    return duration, ratio

def verify_integrity(in_file: str, compressed_file: str) -> bool:
    """Decompress with 'seq' and MD5-check against original."""
    out = "check_integrity.bin"
    if os.path.exists(out):
        os.remove(out)
    cmd = [_exe_path(), "-mode", "decompress", "-in", compressed_file, "-out", out, "-impl", "seq"]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        if os.path.exists(out):
            os.remove(out)
        return False
    try:
        ok = _md5sum(in_file) == _md5sum(out)
    finally:
        if os.path.exists(out):
            os.remove(out)
    return ok

def delete_all_pcz(tracked: List[str]) -> None:
    """Tracked delete + sweep any *.pcz left."""
    for path in tracked:
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except OSError:
                pass
    for path in glob.glob("*.pcz"):
        try:
            os.remove(path)
        except OSError:
            pass

# ---- Plotting ----
def plot_speedup_for_impl(dataset_key: str, dataset_label: str,
                          impl_key: str, impl_label: str,
                          thread_counts: List[int], speedups: List[float]) -> str:
    plt.figure(figsize=(10, 6))
    plt.plot(thread_counts, speedups, marker="o", linestyle="-", label=impl_label)
    plt.axhline(y=1.0, linestyle=":", label="Sequential baseline")
    plt.title(f"Speedup: {impl_label} on {dataset_label} ({FILE_SIZE_MB}MB)")
    plt.xlabel("Number of Threads")
    plt.ylabel("Speedup Factor")
    plt.xticks(thread_counts)
    plt.legend()
    plt.grid(True)
    out_path = f"speedup_{impl_key}_{dataset_key}.png"
    plt.savefig(out_path, bbox_inches="tight")
    plt.close()
    return out_path

def plot_speedup_comparison(dataset_key: str, dataset_label: str,
                            results: Dict[str, List[float]], labels: Dict[str, str]) -> str:
    plt.figure(figsize=(10, 6))
    for impl_key, speedups in results.items():
        plt.plot(THREAD_COUNTS, speedups, marker="o", linestyle="-", label=labels[impl_key])
    plt.axhline(y=1.0, linestyle=":", label="Sequential baseline")
    plt.title(f"Speedup Comparison on {dataset_label} ({FILE_SIZE_MB}MB)")
    plt.xlabel("Number of Threads")
    plt.ylabel("Speedup Factor")
    plt.xticks(THREAD_COUNTS)
    plt.legend()
    plt.grid(True)
    out_path = f"speedup_comparison_{dataset_key}.png"
    plt.savefig(out_path, bbox_inches="tight")
    plt.close()
    return out_path

# ---- Orchestrator ----
def main() -> None:
    compile_go()

    tracked_artifacts: List[str] = []
    images_written: List[str] = []

    for dataset_key, dataset_label in DATASETS:
        in_file = _dataset_path(dataset_key)
        GENERATOR_BY_KEY[dataset_key](in_file, FILE_SIZE_MB)

        # Baseline sequential for this dataset
        seq_out = f"output_seq_{dataset_key}.pcz"
        seq_time, _ = run_compress(in_file, "seq", 1, output_file=seq_out)
        tracked_artifacts.append(seq_out)

        if not verify_integrity(in_file, seq_out):
            delete_all_pcz(tracked_artifacts)
            raise SystemExit(1)

        # Collect speedups
        speedups_by_impl: Dict[str, List[float]] = {}
        labels_by_impl: Dict[str, str] = {}

        for impl_key, impl_label in PARALLEL_IMPLS:
            labels_by_impl[impl_key] = impl_label
            impl_speedups: List[float] = []

            for t in THREAD_COUNTS:
                total_time = 0.0
                last_ratio = 0.0
                for i in range(ITERATIONS):
                    out_file = f"output_{impl_key}_{dataset_key}_{t}_{i}.pcz"
                    dur, ratio = run_compress(in_file, impl_key, t, output_file=out_file)
                    total_time += dur
                    last_ratio = ratio
                    tracked_artifacts.append(out_file)
                avg_time = total_time / ITERATIONS
                impl_speedups.append(seq_time / avg_time if avg_time > 0 else 0.0)

            speedups_by_impl[impl_key] = impl_speedups
            img = plot_speedup_for_impl(dataset_key, dataset_label, impl_key, impl_label, THREAD_COUNTS, impl_speedups)
            images_written.append(img)

        comp_img = plot_speedup_comparison(dataset_key, dataset_label, speedups_by_impl, labels_by_impl)
        images_written.append(comp_img)

    delete_all_pcz(tracked_artifacts)

if __name__ == "__main__":
    main()