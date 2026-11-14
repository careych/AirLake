"""Lossless JSON progress values; fractional Decimal metrics remain decimal strings."""

import math
from datetime import UTC, date, datetime
from decimal import Decimal


def normalize(value):
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Non-finite progress number")
        # JSON integer text is exact. Do not round decimal fractions through float.
        return int(value) if value == value.to_integral_value() else format(value, "f")
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Progress timestamps require an explicit timezone")
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("JSON progress keys must be strings")
        return {key: normalize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("Non-finite progress number")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError("Unsupported progress type")
