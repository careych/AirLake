"""Pure-Python tests of REAL preflight/batch/MERGE control flow; no Spark imports.

Only those functions' ASTs are compiled, with inert dependencies supplied below.
This verifies orchestration, not Spark predicates, arithmetic, or Iceberg behavior.
Known defects remain normal failures, not expectedFailure/xfail annotations.
"""

import __future__

import ast
import unittest
from pathlib import Path
from types import CodeType, FunctionType
from unittest.mock import Mock


class _Expression:
    """Inert placeholder: expressions are never evaluated against real data."""

    def isNull(self):
        return self

    def isNotNull(self):
        return self

    def rlike(self, pattern):
        return self

    def __or__(self, other):
        return self

    def __and__(self, other):
        return self

    def __eq__(self, other):
        return self

    def __invert__(self):
        return self

    def eqNullSafe(self, other):
        return self

    def __lt__(self, other):
        return self

    def __gt__(self, other):
        return self


def _frame(name, count=0):
    frame = Mock(name=name)
    frame.count.return_value = count
    frame.filter.return_value.count.return_value = 0
    frame.filter.return_value.limit.return_value.count.return_value = 0
    frame.filter.return_value.select.return_value.limit.return_value.count.return_value = 0
    frame.filter.return_value.select.return_value.join.return_value.limit.return_value.count.return_value = 0
    frame.select.return_value.distinct.return_value.join.return_value.count.return_value = 0
    frame.groupBy.return_value.count.return_value.filter.return_value.count.return_value = 0
    frame.alias.return_value.join.return_value.filter.return_value.count.return_value = 1
    return frame


def _production_function(namespace, name):
    path = Path(__file__).resolve().parents[1] / "scripts" / "gold_batch.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == name
    )
    module = ast.Module(body=[function], type_ignores=[])
    # Postponed annotations avoid resolving SparkSession; imports/main never run.
    code = compile(
        module, str(path), "exec", flags=__future__.annotations.compiler_flag
    )
    function_code = next(
        constant
        for constant in code.co_consts
        if isinstance(constant, CodeType) and constant.co_name == name
    )
    result = FunctionType(function_code, namespace)
    result.__kwdefaults__ = {
        argument.arg: ast.literal_eval(value)
        for argument, value in zip(function.args.kwonlyargs, function.args.kw_defaults)
        if value is not None
    }
    return result


class GoldP0OrchestrationTests(unittest.TestCase):
    def setUp(self):
        self.events = []
        self.dimension = _frame("dimension target", count=1)
        self.hourly = _frame("hourly target", count=1)
        self.dimension_source = _frame("dimension source", count=1)
        self.hourly_source = _frame("hourly source", count=1)
        self.spark = Mock(name="inert Spark stand-in")
        self.spark.table.side_effect = {
            "dimension": self.dimension,
            "hourly": self.hourly,
        }.__getitem__

        def sql(statement):
            self.events.append(f"MERGE {statement.split()[2]}")

        self.spark.sql.side_effect = sql

        self.namespace = {
            "F": Mock(col=Mock(return_value=_Expression())),
            "DIMENSION_TABLE": "dimension",
            "HOURLY_TABLE": "hourly",
            "DIMENSION_SCHEMA": [],
            "HOURLY_SCHEMA": [],
            "BUSINESS_TYPES": {4946812: "reference"},
            "assert_schema": Mock(),
            "ensure_silver_source": Mock(return_value=_frame("Silver")),
            "build_dimension_source": Mock(return_value=self.dimension_source),
            "ensure_breakpoint_table": Mock(return_value=_frame("breakpoints")),
            "build_hourly_aqi_source": Mock(return_value=(self.hourly_source, {})),
            "create_gold_tables": Mock(),
            "assert_target_integrity": Mock(),
            "content_digest": Mock(return_value="inert digest"),
            "print": Mock(),
        }
        for name in (
            "_invalid_numeric",
            "_blank_name",
            "_invalid_station_mapping",
            "assert_gold_integrity",
            "merge_if_changed",
        ):
            self.namespace[name] = _production_function(self.namespace, name)
        self.batch = _production_function(self.namespace, "run_gold_batch")

    def assert_no_gold_writes(self):
        self.assertEqual([], self.events)
        self.namespace["create_gold_tables"].assert_not_called()
        for call in self.namespace["ensure_breakpoint_table"].call_args_list:
            self.assertIs(call.kwargs.get("initialize"), False)

    def test_source_coverage_rejection_prevents_gold_writes(self):
        """Expected pass: an actual builder rejection must stop orchestration."""
        self.namespace["build_dimension_source"].side_effect = RuntimeError(
            "Missing Bronze metadata"
        )
        with self.assertRaisesRegex(RuntimeError, "Missing Bronze metadata"):
            self.batch(self.spark)
        self.assert_no_gold_writes()

    def test_numeric_rejection_prevents_gold_writes(self):
        """Expected pass; numeric predicate correctness belongs to integration."""
        self.namespace["build_hourly_aqi_source"].side_effect = RuntimeError(
            "Non-finite PM2.5 aggregate"
        )
        with self.assertRaisesRegex(RuntimeError, "Non-finite PM2.5 aggregate"):
            self.batch(self.spark)
        self.assert_no_gold_writes()

    def test_invalid_dimension_target_is_rejected_before_any_write(self):
        """Target key preflight must reject before entering the write phase."""
        self.namespace["assert_target_integrity"].side_effect = RuntimeError(
            "Invalid dimension target"
        )
        with self.assertRaisesRegex(RuntimeError, "Invalid dimension target"):
            self.batch(self.spark)
        self.assert_no_gold_writes()

    def test_invalid_hourly_target_cannot_leave_partial_dimension_write(self):
        """Hourly target preflight must precede the dimension MERGE."""

        def validate(frame, keys, table):
            if table == "hourly":
                raise RuntimeError("Invalid hourly target")

        self.namespace["assert_target_integrity"].side_effect = validate
        with self.assertRaisesRegex(RuntimeError, "Invalid hourly target"):
            self.batch(self.spark)
        self.assert_no_gold_writes()

    def test_existing_orphan_is_rejected_before_any_write(self):
        """Existing orphan checks must precede both target MERGEs."""
        self.hourly.select.return_value.distinct.return_value.join.return_value.count.return_value = 1
        with self.assertRaisesRegex(RuntimeError, "missing from"):
            self.batch(self.spark)
        self.assert_no_gold_writes()

    def test_invalid_existing_hourly_value_is_rejected_before_any_write(self):
        """Existing output value checks must precede both MERGEs."""
        self.hourly.filter.return_value.count.return_value = 1
        with self.assertRaisesRegex(RuntimeError, "invalid output rows"):
            self.batch(self.spark)
        self.assert_no_gold_writes()

    def test_legacy_repair_metadata_rejection_prevents_write_phase(self):
        self.namespace["build_dimension_source"].side_effect = RuntimeError(
            "Legacy null name is missing Bronze metadata"
        )
        with self.assertRaisesRegex(RuntimeError, "Legacy null name"):
            self.batch(self.spark)
        self.namespace["build_dimension_source"].assert_called_once_with(
            self.spark,
            self.namespace["ensure_silver_source"].return_value,
            self.dimension,
        )
        self.assert_no_gold_writes()

    def test_uncovered_null_name_repair_plan_prevents_write_phase(self):
        self.dimension.filter.return_value.select.return_value.join.return_value.limit.return_value.count.return_value = 1
        with self.assertRaisesRegex(RuntimeError, "without validated Bronze repairs"):
            self.batch(self.spark)
        self.assert_no_gold_writes()

    def test_postflight_rejects_remaining_null_names(self):
        self.dimension.filter.return_value.select.return_value.limit.return_value.count.return_value = 1
        with self.assertRaisesRegex(RuntimeError, "without validated Bronze repairs"):
            self.namespace["assert_gold_integrity"](self.dimension, None)
        self.assert_no_gold_writes()


if __name__ == "__main__":
    unittest.main(verbosity=2)
