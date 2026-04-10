#!/usr/bin/env python3
"""
Merge multiple log streams into one chronological feed.
Reads log files, parses timestamps, and outputs merged sorted entries.
"""

import argparse
import heapq
import itertools
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional, Tuple


DEFAULT_TIMESTAMP_PATTERNS = [
    (r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)", "%Y-%m-%dT%H:%M:%S"),
    (r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)", "%Y-%m-%d %H:%M:%S"),
    (r"(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}(?:\.\d+)?)", "%d/%b/%Y:%H:%M:%S"),
    (r"(\w{3} +\d{1,2} \d{2}:\d{2}:\d{2})", "%b %d %H:%M:%S"),
]


def load_custom_patterns(pattern_file: Optional[Path]) -> list[Tuple[str, str]]:
    """Load custom timestamp patterns from a file."""
    if not pattern_file or not pattern_file.exists():
        return []

    patterns = []
    try:
        with open(pattern_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split("|", 1)
                if len(parts) == 2:
                    regex_pattern, datetime_format = parts
                    patterns.append((regex_pattern.strip(), datetime_format.strip()))
    except Exception as e:
        print(f"Warning: Could not load pattern file {pattern_file}: {e}", file=sys.stderr)

    return patterns


def parse_timestamp(line: str, patterns: list[Tuple[str, str]]) -> Optional[datetime]:
    """Extract and parse timestamp from a log line."""
    for pattern, fmt in patterns:
        match = re.search(pattern, line)
        if match:
            ts_str = match.group(1)
            try:
                ts_str_clean = re.sub(r"(\.\d+)", "", ts_str)
                ts_str_clean = re.sub(r"(Z|[+-]\d{2}:?\d{2})$", "", ts_str_clean)
                ts_str_clean = re.sub(r" +", " ", ts_str_clean).strip()

                parsed = datetime.strptime(ts_str_clean, fmt)

                if fmt.startswith("%b"):
                    current_year = datetime.now().year
                    parsed = parsed.replace(year=current_year)

                return parsed
            except ValueError:
                continue
    return None


def count_lines(filepath: Path) -> int:
    """Count total lines in a file for progress tracking."""
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            return sum(1 for _ in f)
    except Exception:
        return 0


def read_log_file(filepath: Path, patterns: list[Tuple[str, str]]) -> Iterator[Tuple[datetime, str, Path]]:
    """Read log file and yield (timestamp, line, file_index) tuples."""
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            for line_num, line in enumerate(f, 1):
                line = line.rstrip("\n\r")
                if not line.strip():
                    continue
                ts = parse_timestamp(line, patterns)
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


def merge_log_streams(
    filepaths: list[Path],
    patterns: list[Tuple[str, str]],
    output_file: Optional[Path] = None,
    show_progress: bool = False,
) -> None:
    """Merge multiple log files into chronological order."""
    if not filepaths:
        print("No input files provided", file=sys.stderr)
        sys.exit(1)

    total_lines = 0
    if show_progress:
        for fp in filepaths:
            total_lines += count_lines(fp)

    file_iters = []
    for fp in filepaths:
        file_iters.append(read_log_file(fp, patterns))

    sequence = itertools.count()
    heap = []
    for idx, file_iter in enumerate(file_iters):
        try:
            entry = next(file_iter)
            heapq.heappush(heap, (entry[0], next(sequence), idx, entry[1], entry[2]))
        except StopIteration:
            pass

    output_handle = open(output_file, "w", encoding="utf-8") if output_file else sys.stdout

    processed = 0
    last_pct = -1

    try:
        while heap:
            ts, _, idx, line, source = heapq.heappop(heap)
            output_handle.write(f"{line}\n")
            processed += 1

            if show_progress and total_lines > 0:
                pct = int(processed / total_lines * 100)
                if pct != last_pct:
                    print(f"\rProgress: {pct}% ({processed}/{total_lines} lines)", end="", file=sys.stderr)
                    last_pct = pct

            try:
                next_entry = next(file_iters[idx])
                heapq.heappush(heap, (next_entry[0], next(sequence), idx, next_entry[1], next_entry[2]))
            except StopIteration:
                pass

        if show_progress:
            print(f"\rProgress: 100% ({processed}/{total_lines} lines)\n", file=sys.stderr)
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
    parser.add_argument(
        "-p", "--patterns",
        type=Path,
        default=None,
        help="File with custom timestamp patterns (format: regex|datetime_format)"
    )
    parser.add_argument(
        "--pattern",
        action="append",
        dest="inline_patterns",
        metavar="REGEX|FORMAT",
        help="Inline custom timestamp pattern (can be specified multiple times)"
    )
    parser.add_argument(
        "--progress",
        action="store_true",
        help="Show progress indicator during merge"
    )

    args = parser.parse_args()

    patterns = list(DEFAULT_TIMESTAMP_PATTERNS)

    custom_patterns = load_custom_patterns(args.patterns)
    patterns.extend(custom_patterns)

    if args.inline_patterns:
        for inline in args.inline_patterns:
            parts = inline.split("|", 1)
            if len(parts) == 2:
                patterns.append((parts[0].strip(), parts[1].strip()))
            else:
                print(f"Warning: Invalid pattern format '{inline}', expected 'regex|format'", file=sys.stderr)

    valid_files = validate_files(args.files)

    if not valid_files:
        print("Error: No valid input files", file=sys.stderr)
        sys.exit(1)

    if args.verbose:
        print(f"Merging {len(valid_files)} file(s) with {len(patterns)} pattern(s)...", file=sys.stderr)
        for fp in valid_files:
            print(f"  - {fp}", file=sys.stderr)

    merge_log_streams(valid_files, patterns, args.output, show_progress=args.progress)

    if args.verbose:
        if args.output:
            print(f"Output written to: {args.output}", file=sys.stderr)
        else:
            print("Output written to stdout", file=sys.stderr)


if __name__ == "__main__":
    main()
