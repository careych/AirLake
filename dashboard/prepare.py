"""Create a local SQLite development fixture from retained real Gold evidence."""

import argparse
import hashlib
import json
import secrets
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JOINED_SQL = """SELECT f.location_id, d.name AS station_name, f.hour_start AS hour_start_utc,
f.pm25_avg, f.aqi, 'µg/m³' AS units, 'simplified_hourly_mean_mvp' AS aqi_method,
ROUND((julianday('now')-julianday(f.hour_start, '+1 hour'))*24,2) AS age_hours,
CASE WHEN julianday(f.hour_start, '+1 hour')>julianday('now') THEN 'FUTURE'
WHEN (julianday('now')-julianday(f.hour_start, '+1 hour'))*24>24 THEN 'STALE (>24h)'
ELSE 'RECENT (<=24h)' END AS freshness,
'Historical July 2025 acceptance fixture; NOT live AWS queries' AS dataset_label
FROM pm25_aqi_hourly f JOIN dim_location d ON d.location_id=f.location_id"""


def prepare(output):
    output = Path(output).resolve()
    if any((p / ".git").exists() for p in (output, *output.parents)):
        raise ValueError("Runtime must be outside Git")
    output.mkdir(parents=True, exist_ok=False)
    output.chmod(0o700)
    evidence = ROOT / "tests/evidence/real_openaq_2026-11-06.json"
    cloud = ROOT / "tests/evidence/real_openaq_cloud_2026-11-07.json"
    data = json.loads(evidence.read_text())
    db = sqlite3.connect(output / "gold.sqlite")
    db.executescript(
        "CREATE TABLE pm25_aqi_hourly(location_id INTEGER,hour_start TIMESTAMP,pm25_avg REAL,aqi INTEGER,PRIMARY KEY(location_id,hour_start)); CREATE TABLE dim_location(location_id INTEGER PRIMARY KEY,name TEXT,lat REAL,lon REAL,station_type TEXT);"
    )
    for row in data["hourly_rows"]:
        db.execute(
            "INSERT INTO pm25_aqi_hourly VALUES(?,?,?,?)",
            (
                row["location_id"],
                row["hour_start"]
                .replace("T", " ")
                .replace(".000Z", "")
                .replace("Z", ""),
                row["pm25_avg"],
                row["aqi"],
            ),
        )
    for row in data["dimension_rows"]:
        db.execute(
            "INSERT INTO dim_location VALUES(?,?,?,?,?)",
            tuple(
                row[k] for k in ("location_id", "name", "lat", "lon", "station_type")
            ),
        )
    db.execute("CREATE VIEW gold_hourly AS " + JOINED_SQL)
    db.execute(
        "CREATE VIEW gold_latest AS "
        + JOINED_SQL
        + " WHERE f.hour_start=(SELECT MAX(x.hour_start) FROM pm25_aqi_hourly x WHERE x.location_id=f.location_id)"
    )
    db.commit()
    counts = {
        "facts": db.execute("SELECT COUNT(*) FROM gold_hourly").fetchone()[0],
        "stations": db.execute("SELECT COUNT(*) FROM dim_location").fetchone()[0],
        "latest_rows": db.execute("SELECT COUNT(*) FROM gold_latest").fetchone()[0],
    }
    assert counts == {"facts": 23, "stations": 1, "latest_rows": 1}
    db.close()
    private = {
        "SUPERSET_SECRET_KEY": secrets.token_hex(40),
        "ADMIN_PASSWORD": secrets.token_urlsafe(24),
    }
    (output / "private.json").write_text(json.dumps(private))
    (output / "private.json").chmod(0o600)
    config = """import json
from pathlib import Path
private=json.loads(Path('/app/dev/private.json').read_text())
SECRET_KEY=private['SUPERSET_SECRET_KEY']
SQLALCHEMY_DATABASE_URI='sqlite:////app/dev/superset.db'
WTF_CSRF_ENABLED=True
TALISMAN_ENABLED=False
PREVENT_UNSAFE_DB_CONNECTIONS=False
FEATURE_FLAGS={'DASHBOARD_NATIVE_FILTERS':True}
DEFAULT_TIME_FILTER='No filter'
ENABLE_PROXY_FIX=False
"""
    (output / "superset_config.py").write_text(config)
    manifest = {
        "mode": "HISTORICAL_ACCEPTANCE_FIXTURE; NOT LIVE ATHENA",
        "source_sha256": {
            str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in [evidence, cloud]
        },
        "counts": counts,
        "timezone": "UTC",
        "freshness_threshold_hours": 24,
        "synthetic_measurements": False,
    }
    (output / "fixture-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    print(json.dumps(prepare(a.output), indent=2))
