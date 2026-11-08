"""Offline configurable Athena URI/SQL validation; no SDK or network."""

import io
import socket
import unittest
from contextlib import redirect_stdout
from dataclasses import replace
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from dashboard.athena_config import SQL, AthenaSettings, main


class AthenaDashboardConfigTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch.object(socket, "socket", side_effect=AssertionError("Offline only"))
        )

    def test_defaults_preserve_reviewed_queries(self):
        settings = AthenaSettings()
        self.assertEqual((SQL / "athena_gold.sql").read_text(), settings.render())
        self.assertEqual(
            (SQL / "athena_latest.sql").read_text(), settings.render("latest")
        )

    def test_identifiers_workgroup_region_and_profile_are_configurable(self):
        settings = replace(
            AthenaSettings(),
            catalog="ApprovedCatalog",
            database="approved_gold",
            hourly_table="facts",
            dimension_table="stations",
            workgroup="approved-workgroup",
            region="ap-southeast-2",
            profile="approved-profile",
            hourly_snapshot="123",
            dimension_snapshot="456",
        )
        uri = urlparse(settings.sqlalchemy_uri())
        params = parse_qs(uri.query)
        self.assertEqual("/approved_gold", uri.path)
        self.assertEqual(["approved-workgroup"], params["work_group"])
        self.assertEqual(["approved-profile"], params["profile_name"])
        self.assertIn('"ApprovedCatalog"."approved_gold"."facts"', settings.render())
        self.assertIn("FOR VERSION AS OF 123", settings.render())
        self.assertIn("FOR VERSION AS OF 456", settings.render())
        self.assertIsNone(uri.password)

    def test_invalid_identifiers_snapshots_and_credentials_in_uri_reject(self):
        for change in (
            {"database": "gold;DROP TABLE x"},
            {"hourly_snapshot": "1 OR TRUE"},
            {"dimension_snapshot": "0"},
            {"hourly_snapshot": str(2**63)},
            {"results_location": "s3://key:secret@bucket/path/"},
            {"profile": "profile?token=x"},
            {"results_location": "https://example.invalid/"},
            {"results_location": "s3://bucket/path/../other/"},
        ):
            with self.subTest(change=change), self.assertRaises(ValueError):
                replace(AthenaSettings(), **change).validate()

    def test_window_is_closed_hour_bounded_and_utc(self):
        for change in (
            {"start": "2025-07-03T15:01:00Z"},
            {"start": "2025-07-03T15:00:00"},
            {"end": "2025-07-05T15:00:00Z"},
            {"end": "2025-07-03T15:00:00Z"},
        ):
            with self.subTest(change=change), self.assertRaises(ValueError):
                replace(AthenaSettings(), **change).validate()

    def test_configurable_results_are_proposals_not_workgroup_override_claims(self):
        settings = replace(
            AthenaSettings(), results_location="s3://approved-bucket/reviewed-prefix/"
        )
        self.assertEqual(
            ["s3://approved-bucket/reviewed-prefix/"],
            parse_qs(urlparse(settings.sqlalchemy_uri()).query)["s3_staging_dir"],
        )

    def test_substitutions_do_not_cascade_between_valid_identifiers_or_snapshots(self):
        defaults = AthenaSettings()
        settings = replace(
            defaults,
            catalog=defaults.database,
            database="other_gold",
            hourly_snapshot=defaults.dimension_snapshot,
            dimension_snapshot="123",
        )
        sql = settings.render()
        self.assertIn('"' + defaults.database + '"."other_gold"', sql)
        self.assertIn("FOR VERSION AS OF " + defaults.dimension_snapshot + " AS f", sql)
        self.assertIn("FOR VERSION AS OF 123 AS d", sql)

    def test_changed_window_does_not_keep_misleading_july_2025_label(self):
        settings = replace(
            AthenaSettings(), start="2026-11-07T00:00:00Z", end="2026-11-08T00:00:00Z"
        )
        sql = settings.render()
        self.assertNotIn("Historical July 2025 Gold", sql)
        self.assertIn("Configured Gold window 2026-11-07 to 2026-11-08", sql)

    def test_cli_prepares_assets_without_aws_or_optional_driver(self):
        with redirect_stdout(io.StringIO()) as output:
            self.assertEqual(0, main([]))
        self.assertIn("NO AWS REQUESTS", output.getvalue())
        self.assertNotIn("access_key", output.getvalue())
