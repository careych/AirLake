"""Install local Superset charts through authenticated REST; no AWS calls."""

import argparse
import json
from pathlib import Path

import requests

CHARTS = [
    ("PM2.5 hourly trend (UTC)", "gold_hourly", "echarts_timeseries_line", "pm25_avg"),
    ("MVP AQI hourly trend (UTC)", "gold_hourly", "echarts_timeseries_line", "aqi"),
    ("Station comparison — historical means", "gold_hourly", "table", "pm25_avg"),
    ("Latest observation by station (UTC)", "gold_latest", "table", None),
    ("Freshness — age since hour end (hours)", "gold_latest", "table", None),
]


def install(root, url="http://127.0.0.1:18088"):
    if url != "http://127.0.0.1:18088":
        raise ValueError("Only the isolated local Superset target is supported")
    root = Path(root)
    if (root / "dashboard-acceptance.json").exists():
        raise ValueError(
            "Dashboard already installed; import the reviewed bundle instead of duplicating objects"
        )
    private = json.loads((root / "private.json").read_text())
    session = requests.Session()
    r = session.post(
        url + "/api/v1/security/login",
        json={
            "username": "airlake_dev",
            "password": private["ADMIN_PASSWORD"],
            "provider": "db",
            "refresh": True,
        },
        timeout=30,
    )
    r.raise_for_status()
    session.headers["Authorization"] = "Bearer " + r.json()["access_token"]
    r = session.get(url + "/api/v1/security/csrf_token/", timeout=30)
    r.raise_for_status()
    session.headers["X-CSRFToken"] = r.json()["result"]
    session.headers["Referer"] = url + "/"

    def post(path, payload):
        result = session.post(url + "/api/v1/" + path, json=payload, timeout=45)
        if result.status_code not in (200, 201):
            raise RuntimeError(
                f"Local Superset {path} HTTP {result.status_code}: " + result.text[:700]
            )
        return result.json()

    db = post(
        "database/",
        {
            "database_name": "AirLake HISTORICAL fixture — NOT live Athena",
            "sqlalchemy_uri": "sqlite:////app/dev/gold.sqlite",
            "expose_in_sqllab": True,
            "allow_run_async": False,
        },
    )["id"]
    datasets = {}
    for name in ["gold_hourly", "gold_latest"]:
        datasets[name] = post(
            "dataset/", {"database": db, "schema": "main", "table_name": name}
        )["id"]
    dashboard = post(
        "dashboard/",
        {
            "dashboard_title": "AirLake — HISTORICAL July 2025 / UTC / NOT LIVE",
            "slug": "airlake-historical-gold",
            "published": True,
        },
    )["id"]
    ids = []
    checks = []
    for title, table, viz, metric in CHARTS:
        dataset = datasets[table]
        params = {
            "datasource": f"{dataset}__table",
            "viz_type": viz,
            "time_range": "No filter",
            "granularity_sqla": "hour_start_utc",
            "time_grain_sqla": "PT1H",
            "row_limit": 1000,
            "adhoc_filters": [],
        }
        if metric:
            agg = {
                "expressionType": "SIMPLE",
                "column": {"column_name": metric},
                "aggregate": "AVG",
                "label": (
                    "PM2.5 (µg/m³)" if metric == "pm25_avg" else "Hourly MVP AQI"
                ),
            }
            params.update(
                metrics=[agg],
                groupby=["station_name"],
                x_axis="hour_start_utc",
                show_legend=True,
                rich_tooltip=True,
                x_axis_title="Observation hour (UTC)",
                y_axis_title=agg["label"],
            )
        if title.startswith("Station comparison"):
            params.update(
                query_mode="aggregate",
                groupby=["station_name"],
                metrics=[
                    {
                        "expressionType": "SIMPLE",
                        "column": {"column_name": "pm25_avg"},
                        "aggregate": "AVG",
                        "label": "PM2.5 historical mean (µg/m³)",
                    },
                    {
                        "expressionType": "SIMPLE",
                        "column": {"column_name": "aqi"},
                        "aggregate": "AVG",
                        "label": "MVP AQI historical mean",
                    },
                ],
            )
        elif not metric:
            params.update(
                all_columns=[
                    "station_name",
                    "hour_start_utc",
                    "pm25_avg",
                    "aqi",
                    "age_hours",
                    "freshness",
                    "dataset_label",
                ],
                order_by_cols=['["hour_start_utc", false]'],
                query_mode="raw",
            )
        chart = post(
            "chart/",
            {
                "slice_name": title,
                "viz_type": viz,
                "datasource_id": dataset,
                "datasource_type": "table",
                "params": json.dumps(params),
                "dashboards": [dashboard],
            },
        )["id"]
        ids.append(chart)
        query = {
            "datasource": {"id": dataset, "type": "table"},
            "force": True,
            "queries": [
                {
                    "columns": [
                        "station_name",
                        "hour_start_utc",
                        "pm25_avg",
                        "aqi",
                        "age_hours",
                        "freshness",
                    ],
                    "metrics": [],
                    "row_limit": 1000,
                    "time_range": "No filter",
                }
            ],
            "result_format": "json",
            "result_type": "full",
        }
        result = post("chart/data", query)
        rows = result["result"][0]["data"]
        checks.append(
            {"chart_id": chart, "title": title, "authenticated_query_rows": len(rows)}
        )
    positions = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {
            "id": "GRID_ID",
            "type": "GRID",
            "children": [],
            "parents": ["ROOT_ID"],
        },
    }
    for i, chart in enumerate(ids):
        row = "ROW-" + str(i)
        key = "CHART-" + str(chart)
        positions["GRID_ID"]["children"].append(row)
        positions[row] = {
            "id": row,
            "type": "ROW",
            "children": [key],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        positions[key] = {
            "id": key,
            "type": "CHART",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row],
            "meta": {
                "chartId": chart,
                "height": 35 if i < 2 else 22,
                "width": 12,
                "sliceName": CHARTS[i][0],
            },
        }
    filter_config = {
        "native_filter_configuration": [
            {
                "id": "station-filter",
                "name": "Station",
                "filterType": "filter_select",
                "targets": [
                    {
                        "datasetId": datasets["gold_hourly"],
                        "column": {"name": "station_name"},
                    }
                ],
                "defaultDataMask": {"extraFormData": {}, "filterState": {}},
                "controlValues": {
                    "enableEmptyFilter": False,
                    "multiSelect": True,
                    "searchAllOptions": False,
                    "inverseSelection": False,
                },
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                "type": "NATIVE_FILTER",
            }
        ]
    }
    update = session.put(
        url + f"/api/v1/dashboard/{dashboard}",
        json={
            "position_json": json.dumps(positions),
            "json_metadata": json.dumps(filter_config),
        },
        timeout=30,
    )
    update.raise_for_status()
    exported = session.get(
        url + f"/api/v1/dashboard/export/?q={json.dumps([dashboard])}", timeout=30
    )
    exported.raise_for_status()
    (root / "dashboard-export.zip").write_bytes(exported.content)
    report = {
        "mode": "ACTUAL LOCAL SUPERSET; HISTORICAL FIXTURE; NOT LIVE ATHENA",
        "dashboard_url": url + "/superset/dashboard/airlake-historical-gold/",
        "chart_checks": checks,
        "dashboard_id": dashboard,
        "dataset_ids": datasets,
        "station_count": 1,
        "time_zone": "UTC",
        "athena_queries_executed": 0,
    }
    (root / "dashboard-acceptance.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, required=True)
    a = p.parse_args()
    print(json.dumps(install(a.root), indent=2))
