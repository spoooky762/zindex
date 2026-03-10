#!/usr/bin/env python3

import argparse
import io
import json
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a chunk-indexed .tar.zst archive from .txt files."
    )
    parser.add_argument("input_dir", type=Path, help="Directory containing .txt files")
    parser.add_argument(
        "output",
        type=Path,
        help="Output archive path. Use a .tar.zst suffix.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1024 * 1024,
        help="Uncompressed bytes per zstd frame (default: 1048576)",
    )
    parser.add_argument(
        "--level",
        type=int,
        default=9,
        help="zstd compression level (default: 9)",
    )
    return parser.parse_args()


def collect_txt_files(input_dir: Path) -> list[Path]:
    files = sorted(path for path in input_dir.rglob("*.txt") if path.is_file())
    if not files:
        raise SystemExit(f"no .txt files found under {input_dir}")
    return files


def normalize_output_path(output: Path) -> Path:
    if output.name.endswith(".tar.zst"):
        return output
    return output.with_name(f"{output.name}.tar.zst")


def build_tar_bytes(input_dir: Path, files: list[Path]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT) as tf:
        for path in files:
            arcname = path.relative_to(input_dir).as_posix()
            info = tf.gettarinfo(str(path), arcname=arcname)
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            info.mtime = 0
            with path.open("rb") as fh:
                tf.addfile(info, fh)
    return buf.getvalue()


def compress_chunk(chunk: bytes, level: int) -> bytes:
    zstd = shutil.which("zstd")
    if not zstd:
        raise SystemExit("zstd CLI not found in PATH")
    try:
        result = subprocess.run(
            [zstd, "--compress", "--stdout", "--quiet", f"-{level}"],
            input=chunk,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.decode("utf-8", errors="replace").strip()
        raise SystemExit(f"zstd failed: {stderr or exc}") from exc
    return result.stdout


def write_archive_and_index(tar_bytes: bytes, output: Path, chunk_size: int, level: int) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    index_path = output.with_name(f"{output.name}.json")

    chunks = []
    compressed_offset = 0
    with output.open("wb") as archive_fh:
        for chunk_id, start in enumerate(range(0, len(tar_bytes), chunk_size)):
            chunk = tar_bytes[start : start + chunk_size]
            compressed = compress_chunk(chunk, level)
            archive_fh.write(compressed)
            chunks.append(
                {
                    "chunk_id": chunk_id,
                    "compressed_offset": compressed_offset,
                    "compressed_size": len(compressed),
                    "uncompressed_start": start,
                    "uncompressed_end": start + len(chunk),
                }
            )
            compressed_offset += len(compressed)

    with index_path.open("w", encoding="utf-8") as fh:
        json.dump({"chunks": chunks}, fh, indent=2)
        fh.write("\n")

    return index_path


def main() -> int:
    args = parse_args()
    input_dir = args.input_dir.resolve()
    output = normalize_output_path(args.output.resolve())

    if args.chunk_size <= 0:
        raise SystemExit("--chunk-size must be positive")
    if not input_dir.is_dir():
        raise SystemExit(f"input directory does not exist: {input_dir}")

    files = collect_txt_files(input_dir)
    tar_bytes = build_tar_bytes(input_dir, files)
    index_path = write_archive_and_index(tar_bytes, output, args.chunk_size, args.level)

    print(output)
    print(index_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
