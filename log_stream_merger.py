#!/usr/bin/env python3
"""
Merge multiple log streams into one chronological feed.
Reads log files, parses timestamps, and outputs merged sorted entries.
"""

import argparse
import heapq
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional, Tuple


TIMESTAMP_PATTERNS = [
    (r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)", "%Y-%m-%dT%H:%M:%S"),
    (r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)", "%Y-%m-%d %H:%M:%S"),
    (r"(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}(?:\.\d+)?)", "%d/%b/%Y:%H:%M:%S"),
    (r"(\w{3} +\d{1,2} \d{2}:\d{2}:\d{2})", "%b %d %H:%M:%S"),
]


def parse_timestamp(line: str) -> Optional[datetime]:
    """Extract and parse timestamp from a log line."""
    for pattern, fmt in TIMESTAMP_PATTERNS:
        match = re.search(pattern, line)
        if match:
            ts_str = match.group(1)
            try:
                if "T" in ts_str or "-" in ts_str[:4]:
                    ts_str_clean = re.sub(r"(\.\d+)", "", ts_str)
                    ts_str_clean = re.sub(r"(Z|[+-]\d{2}:?\d{2})$", "", ts_str_clean)
                    return datetime.strptime(ts_str_clean, fmt.split("(")[0].strip())
                elif "/" in ts_str:
                    ts_str_clean = re.sub(r"(\.\d+)", "", ts_str)
                    return datetime.strptime(ts_str_clean, fmt)
                else:
                    ts_str_clean = re.sub(r" +", " ", ts_str)
                    parsed = datetime.strptime(ts_str_clean, fmt)
                    current_year = datetime.now().year
                    return parsed.replace(year=current_year)
            except ValueError:
                continue
    return None


def read_log_file(filepath: Path) -> Iterator[Tuple[datetime, str, int]]:
    """Read log file and yield (timestamp, line, file_index) tuples."""
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            for line_num, line in enumerate(f, 1):
                line = line.rstrip("\n\r")
                if not line.strip():
                    continue
                ts = parse_timestamp(line)
                if ts:
                    yield (ts, line, filepath)
                else:
                    yield (datetime.min, f"[UNPARSED] {line}", filepath)
    except FileNotFoundError:
        print(f"Warning: File not found: {filepath}", file=sys.stderr)
    except PermissionError:
        print(f"Warning: Permission denied: {filepath}", file=sys.stderr)
    except Exception as e:
        print(f"Warning: Error reading {filepath}: {e}", file=sys.stderr)


def merge_log_streams(filepaths: list[Path], output_file: Optional[Path] = None) -> None:
    """Merge multiple log files into chronological order."""
    if not filepaths:
        print("No input files provided", file=sys.stderr)
        sys.exit(1)

    file_iters = []
    for fp in filepaths:
        file_iters.append(read_log_file(fp))

    heap = []
    for idx, file_iter in enumerate(file_iters):
        try:
            entry = next(file_iter)
            heapq.heappush(heap, (entry[0], idx, entry[1], entry[2]))
        except StopIteration:
            pass

    output_handle = open(output_file, "w", encoding="utf-8") if output_file else sys.stdout

    try:
        while heap:
            ts, idx, line, source = heapq.heappop(heap)
            output_handle.write(f"{line}\n")

            try:
                next_entry = next(file_iters[idx])
                heapq.heappush(heap, (next_entry[0], idx, next_entry[1], next_entry[2]))
            except StopIteration:
                pass
    finally:
        if output_file:
            output_handle.close()


def validate_files(filepaths: list[Path]) -> list[Path]:
    """Validate that provided paths exist and are readable."""
    valid = []
    for fp in filepaths:
        if fp.exists() and fp.is_file():
            valid.append(fp)
        else:
            print(f"Skipping invalid path: {fp}", file=sys.stderr)
    return valid


def main() -> None:
    """Main entry point for log stream merger."""
    parser = argparse.ArgumentParser(
        description="Merge multiple log streams into one chronological feed"
    )
    parser.add_argument(
        "files",
        nargs="+",
        type=Path,
        help="Log files to merge"
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default=None,
        help="Output file (default: stdout)"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show processing details"
    )

    args = parser.parse_args()

    valid_files = validate_files(args.files)

    if not valid_files:
        print("Error: No valid input files", file=sys.stderr)
        sys.exit(1)

    if args.verbose:
        print(f"Merging {len(valid_files)} file(s)...", file=sys.stderr)
        for fp in valid_files:
            print(f"  - {fp}", file=sys.stderr)

    merge_log_streams(valid_files, args.output)

    if args.verbose:
        if args.output:
            print(f"Output written to: {args.output}", file=sys.stderr)
        else:
            print("Output written to stdout", file=sys.stderr)


if __name__ == "__main__":
    main()
