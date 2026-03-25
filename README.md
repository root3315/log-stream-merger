# log-stream-merger

Merge multiple log streams into one chronological feed.

## Why I Built This

Ever had logs from multiple services and needed to see what actually happened in order? Yeah, me too. This tool takes a bunch of log files, figures out the timestamps, and spits out one merged timeline.

## Quick Start

```bash
python log_stream_merger.py app.log nginx.log worker.log -o merged.log
```

That's it. All your logs, sorted by time.

## Supported Timestamp Formats

The script recognizes these common formats:

- ISO 8601: `2024-01-15T10:30:45.123Z`
- Standard: `2024-01-15 10:30:45.123`
- Apache/Nginx: `15/Jan/2024:10:30:45.123`
- Syslog: `Jan 15 10:30:45`

Lines without recognizable timestamps get prefixed with `[UNPARSED]` and sorted to the top.

## Custom Timestamp Formats

If your logs use a non-standard timestamp format, you can add custom patterns.

### Using a Pattern File

Create a file with custom patterns, one per line:

```
# Custom patterns file
# Format: regex_pattern|datetime_format
(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})|%Y/%m/%d %H:%M:%S
\[([\d-]+ \d+:\d+:\d+)\]|%Y-%m-%d %H:%M:%S
```

Then use it with:

```bash
python log_stream_merger.py -p custom_patterns.txt *.log -o merged.log
```

### Using Inline Patterns

Add patterns directly on the command line:

```bash
python log_stream_merger.py --pattern '(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})|%Y/%m/%d %H:%M:%S' *.log
```

Multiple `--pattern` options can be specified.

## Usage

```
usage: log_stream_merger.py [-h] [-o OUTPUT] [-v] [-p PATTERNS] [--pattern REGEX|FORMAT] files [files ...]

Merge multiple log streams into one chronological feed

positional arguments:
  files                 Log files to merge

options:
  -h, --help            Show help message
  -o, --output OUTPUT   Output file (default: stdout)
  -v, --verbose         Show processing details
  -p, --patterns FILE   File with custom timestamp patterns
  --pattern REGEX|FORMAT  Inline custom pattern (can be repeated)
```

## Examples

Merge and print to stdout:
```bash
python log_stream_merger.py service1.log service2.log
```

Merge and save to file:
```bash
python log_stream_merger.py *.log -o all_merged.log
```

Verbose mode to see what's happening:
```bash
python log_stream_merger.py app.log db.log -v
```

Use custom timestamp patterns:
```bash
python log_stream_merger.py -p my_patterns.txt *.log -o merged.log
python log_stream_merger.py --pattern '(\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2})|%d-%m-%Y %H:%M:%S' *.log
```

## Installation

```bash
pip install -r requirements.txt
```

Or just run it directly - no external dependencies needed.

## Notes

- Uses a heap-based merge algorithm (efficient for many files)
- Handles UTF-8 with fallback for weird encodings
- Empty lines are skipped
- Missing files are warned about but don't crash the script

## License

Do whatever you want with it.
