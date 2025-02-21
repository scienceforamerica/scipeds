import unittest

import pandas as pd
from pandas.testing import assert_frame_equal

from scipeds.data.enums import Gender
from scipeds.utils import Rate, calculate_effect_size, calculate_rel_rate


class CalculationTests(unittest.TestCase):
    def setUp(self):
        """Create a fake result aggregation with three schools:
        one big school, one medium-size school, one small school

        Big school is over-represented in a field by 1.1x, small school is under-represented by 2x,
        medium school is at parity
        Use gender, for now
        TODO: Add tests for other fields / groupings"""
        unitids = [1, 2, 3]
        names = ["Big School", "Medium School", "Small School"]
        gender = Gender.women
        uni_degrees_total = [10_000, 1_000, 100]
        uni_pct_gender = [0.4, 0.5, 0.6]
        uni_degrees_within_gender = [
            int(uni_degrees * pct) for uni_degrees, pct in zip(uni_degrees_total, uni_pct_gender)
        ]
        field_pct_uni = [0.2, 0.15, 0.25]
        field_degrees_total = [
            int(uni_degrees * field_pct)
            for uni_degrees, field_pct in zip(uni_degrees_total, field_pct_uni)
        ]

        self.rel_rate = [0.8, 1, 1.2]
        field_pct_gender = [uni_pct * rr for uni_pct, rr in zip(uni_pct_gender, self.rel_rate)]

        field_degrees_within_gender = [
            int(field_degrees * pct)
            for field_degrees, pct in zip(field_degrees_total, field_pct_gender)
        ]

        self.parity_effect_size = [
            fdwg - int(fdt * upg)
            for fdwg, fdt, upg in zip(
                field_degrees_within_gender, field_degrees_total, uni_pct_gender
            )
        ]

        self.result_df = pd.DataFrame.from_dict(
            {
                "institution_name": names,
                "unitid": unitids,
                "gender": [gender.value] * 3,
                "rollup_degrees_within_gender": field_degrees_within_gender,
                "rollup_degrees_total": field_degrees_total,
                "uni_degrees_within_gender": uni_degrees_within_gender,
                "uni_degrees_total": uni_degrees_total,
            },
        )

        self.rollup_pct = Rate(
            "rollup_pct",
            numerator="rollup_degrees_within_gender",
            denominator="rollup_degrees_total",
        )
        self.uni_pct = Rate(
            "uni_pct", numerator="uni_degrees_within_gender", denominator="uni_degrees_total"
        )

    def _check_result(self, result: pd.DataFrame, expected: pd.DataFrame):
        """Convenience wrapper for checking accurate query return values

        We ignore categorical values and dtypes"""
        n_cols = expected.shape[1]
        assert_frame_equal(
            result.iloc[:, :n_cols],
            expected,
            check_categorical=False,
            check_index_type=False,
        )

    def test_relative_rate(self):
        rr_df = calculate_rel_rate(
            df=self.result_df, subgroup=self.rollup_pct, baseline=self.uni_pct
        )
        assert all(col in rr_df.columns for col in ["rel_rate", "log2_rel_rate", "fold_rel_rate"])

        actual_values = rr_df["rel_rate"].tolist()
        for actual, expected in zip(actual_values, self.rel_rate):
            self.assertAlmostEqual(actual, expected)

        actual_values = rr_df["excess_degrees_from_parity"].tolist()
        for actual, expected in zip(actual_values, self.parity_effect_size):
            self.assertAlmostEqual(actual, expected)

    def test_effect_size(self):
        rr_df = calculate_rel_rate(
            df=self.result_df, subgroup=self.rollup_pct, baseline=self.uni_pct
        )
        es_df = calculate_effect_size(
            df=rr_df,
            field_pct=self.rollup_pct,
            uni_pct=self.uni_pct,
            group_cols=["gender"],
        )

        # median rel rate should be 1
        assert (es_df["median_rel_rate_uni"] == 1).all()

        # assert
        actual_values = es_df["excess_degrees_from_median_expected"].tolist()
        for actual, expected in zip(actual_values, self.parity_effect_size):
            self.assertAlmostEqual(actual, expected)
