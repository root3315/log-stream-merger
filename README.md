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

## Timezone Support

Timestamps with timezone offsets (e.g. `+05:30`, `-08:00`, `Z`) are parsed and converted to UTC for correct chronological ordering. By default, output is in UTC.

### Output Timezone

Use `--tz` to display timestamps in a specific timezone:

```bash
# Output in US Eastern (EST = UTC-5)
python log_stream_merger.py --tz -05:00 app.log -o merged.log

# Output in IST (UTC+5:30)
python log_stream_merger.py --tz +05:30 app.log -o merged.log
```

### Preserve Original Timezones

Use `--preserve-tz` to keep the original timezone offsets in the output:

```bash
python log_stream_merger.py --preserve-tz us.log eu.log -o merged.log
```

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
usage: log_stream_merger.py [-h] [-o OUTPUT] [-v] [-p PATTERNS] [--pattern REGEX|FORMAT] [--tz TZ] [--preserve-tz] [--progress] files [files ...]

Merge multiple log streams into one chronological feed

positional arguments:
  files                 Log files to merge

options:
  -h, --help            Show help message
  -o, --output OUTPUT   Output file (default: stdout)
  -v, --verbose         Show processing details
  -p, --patterns FILE   File with custom timestamp patterns
  --pattern REGEX|FORMAT  Inline custom pattern (can be repeated)
  --tz TZ               Output timezone (UTC, +HH:MM, -HH:MM). Default: UTC
  --preserve-tz         Preserve original timezone offsets in output
  --progress            Show progress indicator during merge
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

Show progress for large merges:
```bash
python log_stream_merger.py --progress *.log -o merged.log
```

Merge logs from different timezones, output in local time:
```bash
python log_stream_merger.py --tz +05:30 us.log eu.log asia.log -o merged.log
```

Merge while keeping original timezone offsets:
```bash
python log_stream_merger.py --preserve-tz *.log -o merged.log
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
