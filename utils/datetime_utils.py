from datetime import datetime, timezone


def as_naive_utc(dt: datetime) -> datetime:
    """Normalize a datetime for comparison against the app's timezone-naive
    TIMESTAMP columns: tz-aware input is converted to UTC and stripped of its
    tzinfo (asyncpg refuses aware datetimes on naive columns); naive input is
    assumed to already be UTC, per the app-wide convention.
    """
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt
