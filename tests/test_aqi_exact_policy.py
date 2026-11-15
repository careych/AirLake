"""Exact AQI rounding policy: source-decimal mean, HALF_UP, order independent."""

import importlib.util
import itertools
import random
import unittest
from fractions import Fraction
from pathlib import Path

# Load privately: importing scripts.gold_batch would leak into other tests' mocks.
_SPEC = importlib.util.spec_from_file_location(
    "_exact_policy_gold_batch",
    Path(__file__).resolve().parents[1] / "scripts/gold_batch.py",
)
gold = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(gold)

# Twelve 5-minute Nhan Chinh readings are not in Git; any 2-decimal set whose
# exact mean is 38.75 reproduces the 2026-09-07T12:00Z half-tie (AQI 77.5).
TIE = ["38.74", "38.76", "38.70", "38.80", "38.65", "38.85"] * 2


class ExactAqiPolicyTests(unittest.TestCase):
    def test_recorded_nhan_chinh_half_tie_rounds_up(self):
        self.assertEqual((Fraction(3875, 100), 78), gold.exact_hourly_aqi(TIE))

    def test_double_summation_order_reproduces_the_recorded_instability(self):
        rng, means = random.Random(1), set()
        for _ in range(2000):
            values = TIE[:]
            rng.shuffle(values)
            total = 0.0
            for value in values:
                total += float(value)
            means.add(total / len(values))
        self.assertIn(38.74999999999999, means)  # recorded cloud mean
        self.assertIn(38.75, means)  # recorded local mean

    def test_every_summation_order_gives_the_same_result(self):
        rng = random.Random(20261115)
        for _ in range(200):
            values = TIE[:]
            rng.shuffle(values)
            self.assertEqual(78, gold.exact_hourly_aqi(values)[1])
            self.assertEqual(78, gold.exact_hourly_aqi(map(float, values))[1])

    def test_just_below_and_above_tie(self):
        self.assertEqual(77, gold.exact_hourly_aqi(["38.749999"])[1])
        self.assertEqual(78, gold.exact_hourly_aqi(["38.75"])[1])
        self.assertEqual(78, gold.exact_hourly_aqi(["38.750001"])[1])

    def test_band_edges_and_cap(self):
        cases = {
            "0": 0,
            "24.99": 50,
            "25": 50,
            "25.01": 50,
            "49.99": 100,
            "50": 100,
            "80": 150,
            "150": 200,
            "250": 300,
            "350": 400,
            "499.99": 500,
            "500": 500,
            "900": 500,
        }
        for value, aqi in cases.items():
            with self.subTest(value=value):
                self.assertEqual(aqi, gold.exact_hourly_aqi([value])[1])

    def test_all_half_ties_in_every_band_round_up(self):
        for a_lo, a_hi, c_lo, c_hi in gold.BREAKPOINTS:
            step = Fraction(c_hi - c_lo) / (a_hi - a_lo)
            for k in range(a_hi - a_lo):
                mean = Fraction(c_lo) + (k + Fraction(1, 2)) * step
                if mean.denominator > 10**6 or 10**6 % mean.denominator:
                    continue
                text = f"{float(mean):.6f}"
                self.assertEqual(a_lo + k + 1, gold.exact_hourly_aqi([text])[1])

    def test_quantization_is_half_up_at_micro_units(self):
        self.assertEqual(Fraction(1, 10**6), gold.exact_hourly_aqi(["0.0000005"])[0])
        self.assertEqual(0, gold.exact_hourly_aqi(["0.0000004"])[0])

    def test_permutations_of_small_set_are_identical(self):
        values = ["12.34", "56.78", "0.01", "99.99"]
        results = {gold.exact_hourly_aqi(p) for p in itertools.permutations(values)}
        self.assertEqual(1, len(results))


if __name__ == "__main__":
    unittest.main()
