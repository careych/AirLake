"""Descriptive historical statistics; no forecasting or official AQI assessment."""

import calendar
import math
from collections import defaultdict
from datetime import timedelta

from historical.data import VIETNAM
from ingestion.openaq import stamp, utc

WHO_REFERENCE = {
    "pm25_24h_micrograms_m3": 15,
    "source": "https://www.who.int/teams/environment-climate-change-and-health/air-quality-and-health/health-impacts/types-of-pollutants",
    "method": "WHO 2021; 24-hour guideline, 99th-percentile context; not a safe/unsafe hourly threshold",
    "applicability": "descriptive Gold hour-bucket means do not establish regulatory or medical compliance",
}


def summarize(rows, start, end, station_ids):
    start, end = utc(start), utc(end)
    if (
        start.minute
        or start.second
        or end.minute
        or end.second
        or not 0 < (end - start).total_seconds() <= 32 * 86400
    ):
        raise ValueError("Whole-hour bounded range required")
    ids = set(station_ids)
    keys = set()
    selected = []
    for r in rows:
        if r["location_id"] not in ids:
            continue
        t = utc(r["hour_start"])
        v = r["pm25_avg"]
        key = (r["location_id"], t)
        if (
            t.minute
            or t.second
            or type(v) not in (int, float)
            or not math.isfinite(v)
            or v < 0
        ):
            raise ValueError("Invalid Gold observation")
        if key in keys:
            raise ValueError("Duplicate Gold key")
        keys.add(key)
        if start <= t < end:
            selected.append({**r, "time": t})
    expected = int((end - start).total_seconds() / 3600)
    result = {}
    first_local = start.astimezone(VIETNAM)
    last_local = end.astimezone(VIETNAM)
    month_days = calendar.monthrange(first_local.year, first_local.month)[1]
    next_month = (first_local.replace(day=28) + timedelta(days=4)).replace(day=1)
    full_month = (
        first_local.day == 1 and first_local.hour == 0 and last_local == next_month
    )
    for loc in sorted(ids):
        data = sorted(
            (r for r in selected if r["location_id"] == loc), key=lambda r: r["time"]
        )
        days = defaultdict(list)
        hours = defaultdict(list)
        slots = {r["time"] for r in data}
        heatmap = []
        for r in data:
            local = r["time"].astimezone(VIETNAM)
            day = local.date().isoformat()
            days[day].append(r)
            hours[local.hour].append(r)
            heatmap.append(
                {
                    "date_local": day,
                    "hour_local": local.hour,
                    "pm25": r["pm25_avg"],
                    "observed_hour_utc": stamp(r["time"]),
                }
            )
        daily = []
        for day, rs in sorted(days.items()):
            mean = math.fsum(r["pm25_avg"] for r in rs) / len(rs)
            if not math.isfinite(mean):
                raise ValueError("Non-finite daily result")
            daily.append(
                {
                    "location_id": loc,
                    "date_local": day,
                    "pm25_avg": mean,
                    "hours_present": len(rs),
                    "hours_expected": 24,
                    "coverage_percent": 100 * len(rs) / 24,
                    "complete_24_hours": len(rs) == 24,
                }
            )
        complete = [r for r in daily if r["complete_24_hours"]]
        weekdays = [
            r for r in complete if utc(r["date_local"] + "T00:00:00Z").weekday() < 5
        ]
        weekends = [
            r for r in complete if utc(r["date_local"] + "T00:00:00Z").weekday() >= 5
        ]
        comparison = {
            "status": "INSUFFICIENT_COMPLETE_DAYS",
            "required_weekdays": 3,
            "required_weekend_days": 2,
        }
        if len(weekdays) >= 3 and len(weekends) >= 2:
            comparison = {
                "status": "DESCRIPTIVE_ONLY",
                "weekday_mean": math.fsum(r["pm25_avg"] for r in weekdays)
                / len(weekdays),
                "weekend_mean": math.fsum(r["pm25_avg"] for r in weekends)
                / len(weekends),
                "weekday_days": len(weekdays),
                "weekend_days": len(weekends),
                "method": "equal weight per complete local calendar day, not a forecast",
            }
        patterns = []
        for hour in range(24):
            rs = hours[hour]
            n = len({r["time"].astimezone(VIETNAM).date() for r in rs})
            patterns.append(
                {
                    "hour_local": hour,
                    "days": n,
                    "pm25_avg": math.fsum(r["pm25_avg"] for r in rs) / len(rs)
                    if n >= 7
                    else None,
                    "required_days": 7,
                }
            )

        def extremum(r):
            return {"observed_hour_utc": stamp(r["time"]), "pm25_avg": r["pm25_avg"]}

        extremes = {
            "lowest_hour": extremum(min(data, key=lambda r: (r["pm25_avg"], r["time"])))
            if data
            else None,
            "highest_hour": extremum(
                max(data, key=lambda r: (r["pm25_avg"], r["time"]))
            )
            if data
            else None,
        }
        month = {
            "status": "INSUFFICIENT_MONTH_COVERAGE",
            "complete_days": len(complete),
            "expected_days": month_days,
            "rule": "full calendar-month selection and at least 80% complete 24-hour days",
        }
        if full_month and len(complete) >= math.ceil(0.8 * month_days):
            month.update(
                status="OBSERVED_COMPLETE_DAYS_ONLY",
                lowest_day=min(
                    complete, key=lambda r: (r["pm25_avg"], r["date_local"])
                ),
                highest_day=max(
                    complete, key=lambda r: (r["pm25_avg"], r["date_local"])
                ),
            )
        rolling = None
        if data:
            tail = data[-1]["time"] + timedelta(hours=1)
            window = [r for r in data if tail - timedelta(hours=24) <= r["time"] < tail]
            if len(window) == 24:
                rolling = {
                    "start_utc": stamp(tail - timedelta(hours=24)),
                    "end_utc": stamp(tail),
                    "hour_count": 24,
                    "pm25_avg": math.fsum(r["pm25_avg"] for r in window) / 24,
                    "interpretation": "descriptive mean of 24 observed Gold hour buckets; not official VN_AQI",
                }
        result[str(loc)] = {
            "status": "AVAILABLE" if data else "NO_DATA",
            "hour_count": len(data),
            "expected_hours": expected,
            "coverage_percent": 100 * len(data) / expected,
            "days_with_data": len(days),
            "complete_days": len(complete),
            "missing_hours_utc": [
                stamp(start + timedelta(hours=i))
                for i in range(expected)
                if start + timedelta(hours=i) not in slots
            ],
            "daily": daily,
            "heatmap": heatmap,
            "hour_of_day": patterns,
            "weekday_weekend": comparison,
            "extremes": extremes,
            "monthly_extremes": month,
            "mean_24h_descriptive": rolling,
        }
    return {
        "start_utc": stamp(start),
        "end_utc": stamp(end),
        "timezone": "Asia/Ho_Chi_Minh",
        "units": "µg/m³",
        "stations": result,
        "interpretation": "descriptive observed periods only; no forecast, city-wide estimate or individualized health advice",
        "who_reference": WHO_REFERENCE,
    }
