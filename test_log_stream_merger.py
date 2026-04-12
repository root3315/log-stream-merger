"""Unit tests for timezone parsing edge cases in log_stream_merger."""

import unittest
from datetime import datetime, timezone, timedelta

from log_stream_merger import parse_offset, parse_output_tz, parse_timestamp, DEFAULT_TIMESTAMP_PATTERNS


class TestParseOffset(unittest.TestCase):
    """Tests for the parse_offset function."""

    def test_utc_zulu(self):
        tz = parse_offset("Z")
        self.assertEqual(tz, timezone.utc)

    def test_positive_offset_with_colon(self):
        tz = parse_offset("+05:30")
        expected = timezone(timedelta(hours=5, minutes=30))
        self.assertEqual(tz, expected)

    def test_negative_offset_with_colon(self):
        tz = parse_offset("-08:00")
        expected = timezone(timedelta(hours=-8))
        self.assertEqual(tz, expected)

    def test_positive_offset_without_colon(self):
        tz = parse_offset("+0530")
        expected = timezone(timedelta(hours=5, minutes=30))
        self.assertEqual(tz, expected)

    def test_negative_offset_without_colon(self):
        tz = parse_offset("-0800")
        expected = timezone(timedelta(hours=-8))
        self.assertEqual(tz, expected)

    def test_positive_hours_only(self):
        tz = parse_offset("+01")
        expected = timezone(timedelta(hours=1))
        self.assertEqual(tz, expected)

    def test_negative_hours_only(self):
        tz = parse_offset("-05")
        expected = timezone(timedelta(hours=-5))
        self.assertEqual(tz, expected)

    def test_plus_zero(self):
        tz = parse_offset("+00:00")
        self.assertEqual(tz, timezone.utc)

    def test_minus_zero(self):
        tz = parse_offset("-00:00")
        self.assertEqual(tz, timezone.utc)

    def test_large_positive_offset(self):
        tz = parse_offset("+14:00")
        expected = timezone(timedelta(hours=14))
        self.assertEqual(tz, expected)

    def test_large_negative_offset(self):
        tz = parse_offset("-12:00")
        expected = timezone(timedelta(hours=-12))
        self.assertEqual(tz, expected)

    def test_half_hour_positive(self):
        tz = parse_offset("+05:45")
        expected = timezone(timedelta(hours=5, minutes=45))
        self.assertEqual(tz, expected)

    def test_half_hour_negative(self):
        tz = parse_offset("-03:30")
        expected = timezone(timedelta(hours=-3, minutes=-30))
        self.assertEqual(tz, expected)


class TestParseOutputTz(unittest.TestCase):
    """Tests for the parse_output_tz function."""

    def test_utc_uppercase(self):
        tz = parse_output_tz("UTC")
        self.assertEqual(tz, timezone.utc)

    def test_utc_lowercase(self):
        tz = parse_output_tz("utc")
        self.assertEqual(tz, timezone.utc)

    def test_utc_mixed_case(self):
        tz = parse_output_tz("Utc")
        self.assertEqual(tz, timezone.utc)

    def test_positive_with_colon(self):
        tz = parse_output_tz("+05:30")
        expected = timezone(timedelta(hours=5, minutes=30))
        self.assertEqual(tz, expected)

    def test_negative_with_colon(self):
        tz = parse_output_tz("-08:00")
        expected = timezone(timedelta(hours=-8))
        self.assertEqual(tz, expected)

    def test_positive_without_colon(self):
        tz = parse_output_tz("+0530")
        expected = timezone(timedelta(hours=5, minutes=30))
        self.assertEqual(tz, expected)

    def test_negative_without_colon(self):
        tz = parse_output_tz("-0800")
        expected = timezone(timedelta(hours=-8))
        self.assertEqual(tz, expected)

    def test_positive_hours_only(self):
        tz = parse_output_tz("+5")
        expected = timezone(timedelta(hours=5))
        self.assertEqual(tz, expected)

    def test_positive_single_digit(self):
        tz = parse_output_tz("+0")
        expected = timezone.utc
        self.assertEqual(tz, expected)

    def test_negative_single_digit(self):
        tz = parse_output_tz("-0")
        expected = timezone.utc
        self.assertEqual(tz, expected)

    def test_invalid_raises_value_error(self):
        with self.assertRaises(ValueError):
            parse_output_tz("INVALID")


class TestParseTimestampTimezone(unittest.TestCase):
    """Tests for parse_timestamp with timezone edge cases."""

    def test_iso_utc_zulu(self):
        ts = parse_timestamp("2024-01-15T10:30:45Z", DEFAULT_TIMESTAMP_PATTERNS)
        self.assertIsNotNone(ts)
        self.assertEqual(ts.tzinfo, timezone.utc)
        self.assertEqual(ts.hour, 10)

    def test_iso_positive_offset(self):
        ts = parse_timestamp("2024-01-15T10:30:45+05:30", DEFAULT_TIMESTAMP_PATTERNS)
        self.assertIsNotNone(ts)
        self.assertEqual(ts.tzinfo, timezone.utc)
        # 10:30:45 +05:30 -> 05:00:45 UTC
        self.assertEqual(ts.hour, 5)
        self.assertEqual(ts.minute, 0)
        self.assertEqual(ts.second, 45)

    def test_iso_negative_offset(self):
        ts = parse_timestamp("2024-01-15T10:30:45-08:00", DEFAULT_TIMESTAMP_PATTERNS)
        self.assertIsNotNone(ts)
        self.assertEqual(ts.tzinfo, timezone.utc)
        # 10:30:45 -08:00 -> 18:30:45 UTC
        self.assertEqual(ts.hour, 18)
        self.assertEqual(ts.minute, 30)

    def test_iso_offset_without_colon(self):
        ts = parse_timestamp("2024-01-15T10:30:45+0530", DEFAULT_TIMESTAMP_PATTERNS)
        self.assertIsNotNone(ts)
        self.assertEqual(ts.tzinfo, timezone.utc)
        self.assertEqual(ts.hour, 5)

    def test_iso_with_milliseconds_and_zulu(self):
        ts = parse_timestamp("2024-01-15T10:30:45.123Z", DEFAULT_TIMESTAMP_PATTERNS)
        self.assertIsNotNone(ts)
        self.assertEqual(ts.tzinfo, timezone.utc)
        # Fractional seconds are stripped during parsing; verify time is correct
        self.assertEqual(ts.hour, 10)
        self.assertEqual(ts.minute, 30)
        self.assertEqual(ts.second, 45)
        self.assertEqual(ts.microsecond, 0)

    def test_iso_with_milliseconds_and_offset(self):
        ts = parse_timestamp("2024-01-15T23:30:45.500+05:30", DEFAULT_TIMESTAMP_PATTERNS)
        self.assertIsNotNone(ts)
        self.assertEqual(ts.tzinfo, timezone.utc)
        # 23:30:45.500 +05:30 -> 18:00:45 UTC (fractional seconds stripped)
        self.assertEqual(ts.hour, 18)
        self.assertEqual(ts.minute, 0)
        self.assertEqual(ts.second, 45)
        self.assertEqual(ts.microsecond, 0)

    def test_iso_no_timezone_assumed_utc(self):
        ts = parse_timestamp("2024-01-15T10:30:45", DEFAULT_TIMESTAMP_PATTERNS)
        self.assertIsNotNone(ts)
        self.assertEqual(ts.tzinfo, timezone.utc)
        self.assertEqual(ts.hour, 10)

    def test_space_separated_no_timezone_assumed_utc(self):
        ts = parse_timestamp("2024-01-15 10:30:45", DEFAULT_TIMESTAMP_PATTERNS)
        self.assertIsNotNone(ts)
        self.assertEqual(ts.tzinfo, timezone.utc)

    def test_daylight_crossing_positive_offset(self):
        ts = parse_timestamp("2024-01-15T02:30:00+05:30", DEFAULT_TIMESTAMP_PATTERNS)
        self.assertIsNotNone(ts)
        self.assertEqual(ts.tzinfo, timezone.utc)
        # 02:30 +05:30 -> 21:00 previous day UTC
        self.assertEqual(ts.hour, 21)
        self.assertEqual(ts.day, 14)

    def test_daylight_crossing_negative_offset(self):
        ts = parse_timestamp("2024-01-15T20:00:00-05:00", DEFAULT_TIMESTAMP_PATTERNS)
        self.assertIsNotNone(ts)
        self.assertEqual(ts.tzinfo, timezone.utc)
        # 20:00 -05:00 -> 01:00 next day UTC
        self.assertEqual(ts.hour, 1)
        self.assertEqual(ts.day, 16)

    def test_midnight_utc(self):
        ts = parse_timestamp("2024-06-15T00:00:00Z", DEFAULT_TIMESTAMP_PATTERNS)
        self.assertIsNotNone(ts)
        self.assertEqual(ts.hour, 0)
        self.assertEqual(ts.minute, 0)

    def test_just_before_midnight(self):
        ts = parse_timestamp("2024-06-15T23:59:59+00:01", DEFAULT_TIMESTAMP_PATTERNS)
        self.assertIsNotNone(ts)
        self.assertEqual(ts.tzinfo, timezone.utc)
        # 23:59:59 +00:01 -> 23:58:59 UTC
        self.assertEqual(ts.hour, 23)
        self.assertEqual(ts.minute, 58)

    def test_no_timestamp_in_line(self):
        ts = parse_timestamp("This is just a plain log message", DEFAULT_TIMESTAMP_PATTERNS)
        self.assertIsNone(ts)

    def test_syslog_format_assumes_utc(self):
        ts = parse_timestamp("Jan  5 10:30:45", DEFAULT_TIMESTAMP_PATTERNS)
        self.assertIsNotNone(ts)
        self.assertEqual(ts.tzinfo, timezone.utc)

    def test_chronological_ordering_across_timezones(self):
        """Verify that two timestamps from different zones sort correctly in UTC."""
        ts_a = parse_timestamp("2024-01-15T10:00:00+05:00", DEFAULT_TIMESTAMP_PATTERNS)
        ts_b = parse_timestamp("2024-01-15T07:00:00+01:00", DEFAULT_TIMESTAMP_PATTERNS)
        self.assertIsNotNone(ts_a)
        self.assertIsNotNone(ts_b)
        # 10:00 +05:00 = 05:00 UTC, 07:00 +01:00 = 06:00 UTC
        self.assertLess(ts_a, ts_b)


if __name__ == "__main__":
    unittest.main()
