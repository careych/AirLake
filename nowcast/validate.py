"""Compare archived CAMS Global PM2.5 with hour-bucket observations.

Archived Open-Meteo values are the most recent model run for each hour (short
lead), so this measures near-analysis agreement, not 1-5 day forecast skill.
"""

import math
from datetime import timedelta

from historical.data import VIETNAM
from ingestion.openaq import utc


def hour_mean(model, hour_start):
    """Model instants at both hour edges approximate an hour-bucket mean."""
    a = model.get(hour_start)
    b = model.get(hour_start + timedelta(hours=1))
    return None if a is None or b is None else (a + b) / 2


def metrics(pairs):
    n = len(pairs)
    if n < 2:
        return {"n": n}
    m = [p for p, _ in pairs]
    o = [q for _, q in pairs]
    mm, mo = math.fsum(m) / n, math.fsum(o) / n
    sm = math.sqrt(math.fsum((x - mm) ** 2 for x in m))
    so = math.sqrt(math.fsum((y - mo) ** 2 for y in o))
    return {
        "n": n,
        "mae": math.fsum(abs(x - y) for x, y in pairs) / n,
        "bias": mm - mo,
        "pearson_r": math.fsum((x - mm) * (y - mo) for x, y in pairs) / (sm * so)
        if sm and so
        else None,
        "observed_mean": mo,
        "model_mean": mm,
    }


def compare(model_series, observations, split_day=15):
    """Hourly and complete-day metrics, plus a time-split ratio-calibration test."""
    model = {utc(t): v for t, v in model_series if v is not None}
    hourly, days, train, test = [], {}, [], []
    for r in observations:
        h = utc(r["observed_hour_utc"])
        m = hour_mean(model, h)
        if m is None:
            continue
        pair = (m, r["pm25"])
        hourly.append(pair)
        local = h.astimezone(VIETNAM)
        days.setdefault(local.date(), []).append(pair)
        (train if local.day <= split_day else test).append(pair)
    daily = [
        (math.fsum(a for a, _ in v) / 24, math.fsum(b for _, b in v) / 24)
        for v in days.values()
        if len(v) == 24
    ]
    calibration = None
    if len(train) >= 48 and len(test) >= 48:
        ratio = math.fsum(o for _, o in train) / math.fsum(m for m, _ in train)
        calibration = {
            "method": f"observed/model ratio fitted on local days 1-{split_day}, tested on later days",
            "ratio": ratio,
            "test_raw": metrics(test),
            "test_calibrated": metrics([(m * ratio, o) for m, o in test]),
        }
    return {
        "hourly": metrics(hourly),
        "daily_complete": metrics(daily),
        "calibration_test": calibration,
    }
