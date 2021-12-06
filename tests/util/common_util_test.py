from util.common_util import fix_timestamp


class TestFixTimestamps:
    def test_aribtrary_strings(self):
        """Test that arbitrary strings aren't fixed."""

        for ts in ("L0A_L_RRST",
                   "This is a test."):
            assert fix_timestamp(ts) == ts

    def test_timestamps_no_ms(self):
        """Test timestamps with no microsecond precision aren't fixed."""

        for ts in ("1998-10-27T00:00:00Z",
                   "1998-10-27T00:00:00"):
            assert fix_timestamp(ts) == ts

    def test_timestamps_up_to_ms(self):
        """Test timestamps with precision up to the microsecond aren't fixed."""

        for ts in ("2000-10-17T00:00:00.Z",
                   "2000-10-17T00:00:00.",
                   "2000-10-17T00:00:00.1Z",
                   "2000-10-17T00:00:00.1",
                   "2000-10-17T00:00:00.12Z",
                   "2000-10-17T00:00:00.12",
                   "2000-10-17T00:00:00.123Z",
                   "2000-10-17T00:00:00.123",
                   "2000-10-17T00:00:00.1234Z",
                   "2000-10-17T00:00:00.1234",
                   "2000-10-17T00:00:00.12345Z",
                   "2000-10-17T00:00:00.12345",
                   "2000-10-17T00:00:00.123456Z",
                   "2000-10-17T00:00:00.123456"):
            assert fix_timestamp(ts) == ts

    def test_timestamps_over_ms(self):
        """Test timestamps with precision greater than a microsecond are fixed."""

        for ts in ("2009-06-13T00:00:00.1234567Z",
                   "2009-06-13T00:00:00.1234567",
                   "2009-06-13T00:00:00.12345678Z",
                   "2009-06-13T00:00:00.12345678",
                   "2009-06-13T00:00:00.123456789Z",
                   "2009-06-13T00:00:00.123456789",
                   "2009-06-13T00:00:00.1234567891Z",
                   "2009-06-13T00:00:00.1234567891",
                   "2009-06-13T00:00:00.123456789123456789Z",
                   "2009-06-13T00:00:00.123456789123456789"):
            if ts.endswith('Z'):
                assert fix_timestamp(ts) == "2009-06-13T00:00:00.123456Z"
            else:
                assert fix_timestamp(ts) == "2009-06-13T00:00:00.123456"
