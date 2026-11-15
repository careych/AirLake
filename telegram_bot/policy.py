"""When a measured PM2.5 reading should trigger an alert (pure, testable).

- Only real observations: never the CAMS model or a carried-forward reading.
- Only new, recent observations (cadence-aware freshness from the projection).
- Sustained: Dragon values are hourly means; a Sensor.Community reading is a
  single ~2.5-minute sample, so two consecutive new readings at least 10
  minutes apart must both reach the threshold.
- One alert per crossing: after an alert the subscription is disarmed until a
  new reading falls below 80% of the threshold, and never more than one alert
  per station every 3 hours.
"""

from datetime import timedelta

from ingestion.openaq import stamp, utc

HYSTERESIS = 0.8
COOLDOWN = timedelta(hours=3)
SAMPLE_GAP = timedelta(minutes=10)
SINGLE_SAMPLE_STATIONS = {99385}


def initial_state():
    return {
        "armed": True,
        "last_observed_at": None,
        "last_alert_at": None,
        "pending_since": None,
    }


def decide(state, observation, threshold, now):
    """Return (alert: bool, new_state). `observation` is one projection record."""
    state = dict(state or initial_state())
    at = observation.get("observed_at")
    value = observation.get("pm25")
    if (
        observation.get("kind") != "observed"
        or observation.get("fetch_status") != "ok"
        or observation.get("freshness", {}).get("status") != "recent"
        or at is None
        or value is None
        or (state["last_observed_at"] and utc(at) <= utc(state["last_observed_at"]))
    ):
        return False, state  # nothing new and current to evaluate
    state["last_observed_at"] = at
    if value < HYSTERESIS * threshold:
        state.update(armed=True, pending_since=None)
        return False, state
    if value < threshold:
        state["pending_since"] = None
        return False, state
    if observation["station_id"] in SINGLE_SAMPLE_STATIONS:
        if state["pending_since"] is None:
            state["pending_since"] = at
            return False, state
        if utc(at) - utc(state["pending_since"]) < SAMPLE_GAP:
            return False, state
    cooled = (
        state["last_alert_at"] is None
        or utc(now) - utc(state["last_alert_at"]) >= COOLDOWN
    )
    if not state["armed"] or not cooled:
        return False, state
    state.update(armed=False, last_alert_at=stamp(utc(now)), pending_since=None)
    return True, state
