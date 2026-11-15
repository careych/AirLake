"""Unit test suite for Trino vs AWS Athena Benchmark (Role C).

Covers:
1. Verification of cost calculation formulas for Athena ($5/TB scanned, 10MB min) and Trino (EC2 runtime).
2. Verification of Athena metrics extraction (DataScannedInBytes, QueryExecutionId, cost, status).
3. Verification of Trino query execution, timing, row counting, and error handling.
4. Verification of Markdown benchmark report generation and summary calculations.
5. Verification of CLI argument parsing and offline dry-run execution.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import MagicMock, patch

REPOSITORY = Path(__file__).resolve().parents[1]


class TestBenchmarkEngines(unittest.TestCase):
    """Test suite for benchmark/benchmark_engines.py with mocked AWS and Trino clients."""

    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location(
            "benchmark_engines", REPOSITORY / "benchmark" / "benchmark_engines.py"
        )
        cls.bm = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.bm)

    def setUp(self):
        # Enforce offline test execution by blocking all live socket calls
        self.enterContext(
            patch("socket.socket", side_effect=AssertionError("No socket calls allowed in unit tests"))
        )
        # Suppress stdout/stderr to keep test output clean
        self.enterContext(contextlib.redirect_stdout(io.StringIO()))
        self.enterContext(contextlib.redirect_stderr(io.StringIO()))

    def test_cost_calculation_formulas(self):
        """Verify Athena scanned data cost and Trino compute cost formulas."""
        # 1. Athena pricing: $5.00 per TB scanned (1 TB = 1024^4 bytes)
        tb_bytes = 1024 ** 4

        # Zero bytes
        self.assertEqual(self.bm.calculate_athena_cost(0), 0.0)
        self.assertEqual(self.bm.calculate_athena_cost(-100), 0.0)

        # 1 TB exact
        cost_1tb = self.bm.calculate_athena_cost(tb_bytes, apply_min_10mb=False)
        self.assertAlmostEqual(cost_1tb, 5.0, places=4)

        # 10 MB minimum enforcement
        mb_5 = 5 * 1024 * 1024
        mb_10 = 10 * 1024 * 1024
        cost_5mb_with_min = self.bm.calculate_athena_cost(mb_5, apply_min_10mb=True)
        cost_10mb = self.bm.calculate_athena_cost(mb_10, apply_min_10mb=False)
        self.assertAlmostEqual(cost_5mb_with_min, cost_10mb, places=6)

        # 5 MB without minimum enforcement
        cost_5mb_no_min = self.bm.calculate_athena_cost(mb_5, apply_min_10mb=False)
        self.assertAlmostEqual(cost_5mb_no_min, cost_10mb / 2.0, places=6)

        # 2. Trino compute cost: hourly_rate / 3600 * seconds
        self.assertEqual(self.bm.calculate_trino_cost(0), 0.0)
        self.assertEqual(self.bm.calculate_trino_cost(-10), 0.0)

        # 1 hour at $0.1008/hr
        cost_1hr = self.bm.calculate_trino_cost(3600.0, hourly_rate_usd=0.1008)
        self.assertAlmostEqual(cost_1hr, 0.1008, places=4)

        # 1800 seconds (30 mins)
        cost_half_hr = self.bm.calculate_trino_cost(1800.0, hourly_rate_usd=0.1008)
        self.assertAlmostEqual(cost_half_hr, 0.0504, places=4)

    def test_athena_metrics_extraction(self):
        """Verify Athena execution extracts DataScannedInBytes, calculates cost, and handles errors."""
        mock_client = MagicMock()
        mock_client.start_query_execution.return_value = {'QueryExecutionId': 'test-query-id-999'}

        # Mock successful query
        mock_client.get_query_execution.return_value = {
            'QueryExecution': {
                'Status': {'State': 'SUCCEEDED'},
                'Statistics': {
                    'DataScannedInBytes': 52428800,  # 50 MB
                    'EngineExecutionTimeInMillis': 1800
                }
            }
        }

        res = self.bm.run_athena_query("SELECT 1", client=mock_client)
        self.assertTrue(res['success'])
        self.assertEqual(res['query_id'], 'test-query-id-999')
        self.assertEqual(res['data_scanned_bytes'], 52428800)
        self.assertEqual(res['data_scanned_mb'], 50.0)
        self.assertGreater(res['cost_usd'], 0.0)
        self.assertIsNone(res['error'])

        # Mock failed query
        mock_client.get_query_execution.return_value = {
            'QueryExecution': {
                'Status': {
                    'State': 'FAILED',
                    'StateChangeReason': 'TABLE_NOT_FOUND: Table pm25_aqi does not exist'
                },
                'Statistics': {'DataScannedInBytes': 0}
            }
        }

        fail_res = self.bm.run_athena_query("SELECT 1", client=mock_client)
        self.assertFalse(fail_res['success'])
        self.assertIn("TABLE_NOT_FOUND", fail_res['error'])
        self.assertEqual(fail_res['data_scanned_bytes'], 0)

        # Mock client connection exception
        mock_client.start_query_execution.side_effect = RuntimeError("AWS Athena unavailable")
        err_res = self.bm.run_athena_query("SELECT 1", client=mock_client)
        self.assertFalse(err_res['success'])
        self.assertIn("AWS Athena unavailable", err_res['error'])

    def test_trino_query_execution_and_timing(self):
        """Verify Trino cursor execution, row count tracking, and error handling."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("loc_1", 25.5), ("loc_2", 30.2)]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_connect_fn = MagicMock(return_value=mock_conn)

        # Successful query run
        res = self.bm.run_trino_query("SELECT * FROM test", connect_fn=mock_connect_fn)
        self.assertTrue(res['success'])
        self.assertEqual(res['row_count'], 2)
        self.assertGreaterEqual(res['execution_time_s'], 0.0)
        self.assertGreaterEqual(res['cost_usd'], 0.0)
        self.assertIsNone(res['error'])

        # Connection error
        failing_connect_fn = MagicMock(side_effect=ConnectionRefusedError("Trino coordinator offline"))
        fail_res = self.bm.run_trino_query("SELECT * FROM test", connect_fn=failing_connect_fn)
        self.assertFalse(fail_res['success'])
        self.assertEqual(fail_res['row_count'], 0)
        self.assertIn("Trino coordinator offline", fail_res['error'])

    def test_markdown_report_generation(self):
        """Verify generated Markdown report contains all summary tables, speedups, and costs."""
        sample_results = [
            {
                "query_name": "1_Truy_van_co_ban",
                "athena_time_s": 2.50,
                "athena_scanned_bytes": 20971520,  # 20 MB
                "athena_scanned_mb": 20.0,
                "athena_cost_usd": 0.000095,
                "trino_time_s": 0.50,
                "trino_rows": 5000,
                "trino_cost_usd": 0.000014
            },
            {
                "query_name": "2_Gom_nhom_don_gian",
                "athena_time_s": 4.00,
                "athena_scanned_bytes": 52428800,  # 50 MB
                "athena_scanned_mb": 50.0,
                "athena_cost_usd": 0.000238,
                "trino_time_s": 1.00,
                "trino_rows": 50,
                "trino_cost_usd": 0.000028
            }
        ]

        metadata = {
            "athena_database": "test_gold_db",
            "trino_host": "10.0.0.50",
            "trino_port": 8080,
            "runs": 2,
            "dry_run": True
        }

        with TemporaryDirectory() as tmpdir:
            output_file = os.path.join(tmpdir, "report.md")
            report_md = self.bm.generate_benchmark_markdown_report(
                sample_results, metadata=metadata, output_path=output_file
            )

            # Check markdown headers and content
            self.assertIn("# Báo Cáo So Sánh Thực Nghiệm Hiệu Năng & Chi Phí: Trino vs AWS Athena", report_md)
            self.assertIn("`test_gold_db`", report_md)
            self.assertIn("`10.0.0.50:8080`", report_md)
            self.assertIn("| 1 | `1_Truy_van_co_ban` | 2.50 | 20.00 | $0.000095 | 0.50 | 5,000 | $0.000014 | **5.00x** |", report_md)
            self.assertIn("| 2 | `2_Gom_nhom_don_gian` | 4.00 | 50.00 | $0.000238 | 1.00 | 50 | $0.000028 | **4.00x** |", report_md)
            self.assertIn("### 2.1. Độ Trễ (Latency)", report_md)
            self.assertIn("### 2.2. Chi Phí (Cost Efficiency)", report_md)

            # Verify file was written
            self.assertTrue(os.path.exists(output_file))
            with open(output_file, "r", encoding="utf-8") as f:
                saved_content = f.read()
            self.assertEqual(saved_content, report_md)

    def test_cli_args_and_dry_run_mode(self):
        """Verify CLI argument parsing and offline dry-run mode completes without network."""
        with TemporaryDirectory() as tmpdir:
            out_file = os.path.join(tmpdir, "dry_run_benchmark.md")
            argv = [
                "--dry-run",
                "--runs", "1",
                "--athena-database", "mock_gold_db",
                "--output", out_file
            ]

            exit_code = self.bm.main(argv)
            self.assertEqual(exit_code, 0)
            self.assertTrue(os.path.exists(out_file))

            with open(out_file, "r", encoding="utf-8") as f:
                content = f.read()
            self.assertIn("mock_gold_db", content)
            self.assertIn("Mô phỏng (Dry-run)", content)
            self.assertIn("1_Truy_van_co_ban", content)
            self.assertIn("2_Gom_nhom_don_gian", content)
            self.assertIn("3_Truy_van_phuc_tap", content)


if __name__ == "__main__":
    unittest.main()
