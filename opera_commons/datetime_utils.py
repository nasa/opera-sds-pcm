"""
Datetime utility functions for OPERA SDS.

This module provides helper functions for consistent datetime handling
across the OPERA SDS codebase, particularly for timezone-aware datetime operations.
"""

from datetime import datetime, timezone
from typing import Union
import dateutil.parser


def ensure_timezone_aware(dt: datetime, default_tz: timezone = timezone.utc) -> datetime:
    """
    Ensure a datetime object is timezone-aware.
    
    If the datetime is already timezone-aware, return it as-is.
    If it's timezone-naive, replace its timezone with the default timezone.
    
    Args:
        dt: The datetime object to make timezone-aware
        default_tz: The timezone to use if dt is timezone-naive (defaults to UTC)
    
    Returns:
        A timezone-aware datetime object
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=default_tz)
    return dt


def parse_iso_datetime(dt_str: str, default_tz: timezone = timezone.utc) -> datetime:
    """
    Parse an ISO datetime string and ensure it's timezone-aware.
    
    This function uses dateutil.parser.isoparse() which can handle various
    ISO datetime formats and will preserve timezone information if present
    in the string. If no timezone information is present, it will use the
    default timezone.
    
    Args:
        dt_str: ISO datetime string to parse
        default_tz: Timezone to use if the string doesn't contain timezone info
    
    Returns:
        A timezone-aware datetime object
    """
    dt = dateutil.parser.isoparse(dt_str)
    return ensure_timezone_aware(dt, default_tz)


def parse_strptime_datetime(dt_str: str, format_str: str, default_tz: timezone = timezone.utc) -> datetime:
    """
    Parse a datetime string using strptime and ensure it's timezone-aware.
    
    Args:
        dt_str: Datetime string to parse
        format_str: strptime format string
        default_tz: Timezone to use (strptime always creates timezone-naive datetimes)
    
    Returns:
        A timezone-aware datetime object
    """
    dt = datetime.strptime(dt_str, format_str)
    return ensure_timezone_aware(dt, default_tz)


def format_datetime_z(dt: datetime) -> str:
    """
    Format a datetime object as ISO string with 'Z' timezone indicator.
    
    This function ensures consistent timestamp formatting across the OPERA SDS
    codebase by always using 'Z' instead of '+00:00' for UTC timezone.
    
    Args:
        dt: The datetime object to format
    
    Returns:
        ISO formatted datetime string with 'Z' timezone indicator
    """
    return dt.replace(tzinfo=None).isoformat() + "Z"


def format_datetime_z_now() -> str:
    """
    Get current UTC time formatted as ISO string with 'Z' timezone indicator.
    
    Convenience function for getting current timestamp in consistent format.
    
    Returns:
        Current UTC time as ISO formatted string with 'Z' timezone indicator
    """
    return format_datetime_z(datetime.now(timezone.utc))
