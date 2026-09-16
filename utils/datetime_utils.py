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


def as_aware_utc(dt: datetime) -> datetime:
    """Normalize a datetime for serialization as an explicit UTC instant: naive
    input is assumed to already be UTC, per the app-wide convention, and gains
    tzinfo; aware input is converted to UTC.

    The counterpart of as_naive_utc(), not a strict inverse. Round-tripping a
    naive value through both returns it unchanged, but as_naive_utc() discards
    the offset of an aware input, so the other direction preserves the instant
    and not the original representation.

    v4 renders timestamps with an explicit designator. Only some of the columns
    it reads are TIMESTAMP WITH TIME ZONE (#720), so without this the same
    response body mixes `...Z` and offset-less values depending on which column
    a field happens to come from.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
