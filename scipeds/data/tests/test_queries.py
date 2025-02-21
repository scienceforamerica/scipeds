import unittest
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from scipeds.constants import CIP_TABLE, COMPLETIONS_TABLE, INSTITUTIONS_TABLE
from scipeds.data.completions import CompletionsQueryEngine
from scipeds.data.enums import (
    AwardLevel,
    FieldTaxonomy,
    Grouping,
    NCSESDetailedFieldGroup,
    NCSESSciGroup,
    RaceEthn,
)
from scipeds.data.queries import QueryFilters, TaxonomyRollup


class QueryEngineTests(unittest.TestCase):
    def setUp(self):
        # We want to replicate the process for creating the duckdb as closely as we can
        # We do this by creating a fake dataset and using the pipeline to write test assets.
        # To re-generate the test db, run `make test-assets`
        test_db = Path(__file__).parents[0] / "assets" / "test.duckdb"

        self.engine = CompletionsQueryEngine(test_db)
        test_completions = self.engine.get_df_from_query(f"SELECT * FROM {COMPLETIONS_TABLE}")
        test_metadata = self.engine.get_df_from_query(
            f"SELECT DISTINCT unitid, institution_name FROM {INSTITUTIONS_TABLE}"
        )
        unis = test_metadata.drop_duplicates().set_index("unitid")
        self.names = unis.to_dict()["institution_name"]
        self.unitids = sorted(test_completions.unitid.unique())
        self.years = sorted(test_completions.year.unique())
        self.genders = sorted(test_completions.gender.unique())
        self.dfgs = sorted(test_completions[FieldTaxonomy.ncses_detailed_field_group].unique())
        self.cips = sorted(test_completions.cipcode.unique())
        self.dhs_stems = [True, False]
        self.race_ethnicities = sorted(test_completions.race_ethnicity.unique())

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

    def test_list_tables(self):
        """Make sure returned list of tables is correct"""
        expected_tables = [
            COMPLETIONS_TABLE,
            CIP_TABLE,
            INSTITUTIONS_TABLE,
        ]
        returned_tables = self.engine.list_tables()
        self.assertCountEqual(expected_tables, returned_tables)

    def test_cip_query(self):
        cip_table = self.engine.get_cip_table()
        assert not cip_table.empty

        # cip code is index, ncses + dhs fields are 5, title col makes 6
        assert len(cip_table.columns) == 6

        returned_cip_titles = cip_table["cip_title"].values.tolist()
        expected_cip_titles = [
            "English Literature (British And Commonwealth)",
            "Mathematics, General",
            "Behavioral Neuroscience",
        ]
        self.assertCountEqual(returned_cip_titles, expected_cip_titles)

    def test_institution_characteristics_query(self):
        inst_table = self.engine.get_institutions_table()
        assert not inst_table.empty

        returned_unitids = inst_table.index
        expected_unitids = [1, 2]
        self.assertCountEqual(returned_unitids, expected_unitids)

        inst_table = self.engine.get_institutions_table(cols=["institution_name"])
        self.assertCountEqual(inst_table.columns, ["institution_name"])
        self.assertEqual(inst_table.index.name, "unitid")

    def test_rollup_check(self):
        good_rollup = TaxonomyRollup(
            taxonomy_name=FieldTaxonomy.ncses_sci_group, taxonomy_values=NCSESSciGroup.sci
        )
        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)
            self.engine._check_rollup_values(good_rollup)

        bad_rollup = TaxonomyRollup(
            taxonomy_name=FieldTaxonomy.ncses_detailed_field_group,
            taxonomy_values=NCSESSciGroup.sci,
        )
        with self.assertWarns(UserWarning):
            self.engine._check_rollup_values(bad_rollup)

    def test_filters(self):
        # Filtering out bachelors should give us an empty dataset
        with self.assertWarnsRegex(UserWarning, "IPEDS"):
            filters = QueryFilters(award_levels=[AwardLevel.masters])
        fields_agg = TaxonomyRollup(
            taxonomy_name=FieldTaxonomy.ncses_sci_group, taxonomy_values=NCSESSciGroup.sci
        )

        result = self.engine.rollup_by_grouping(
            grouping=Grouping.gender, rollup=fields_agg, query_filters=filters
        )
        assert result.empty

        result = self.engine.field_totals_by_grouping(
            grouping=Grouping.gender,
            taxonomy=FieldTaxonomy.ncses_field_group,
            query_filters=filters,
        )
        assert result.empty

        # Wrong year should also give missing values
        with self.assertWarnsRegex(UserWarning, "IPEDS"):
            filters = QueryFilters(start_year=1996, end_year=2021)
        result = self.engine.rollup_by_grouping(
            grouping=Grouping.gender, rollup=fields_agg, query_filters=filters
        )
        assert result.empty

        result = self.engine.field_totals_by_grouping(
            grouping=Grouping.gender,
            taxonomy=FieldTaxonomy.ncses_field_group,
            query_filters=filters,
        )
        assert result.empty

        # Filtering to missing race/ethnicity category should yield nothing
        with self.assertWarnsRegex(UserWarning, "IPEDS"):
            filters = QueryFilters(race_ethns=[RaceEthn.unknown])
        result = self.engine.rollup_by_grouping(
            grouping=Grouping.gender, rollup=fields_agg, query_filters=filters
        )
        assert result.empty

        result = self.engine.field_totals_by_grouping(
            grouping=Grouping.gender,
            taxonomy=FieldTaxonomy.ncses_field_group,
            query_filters=filters,
        )
        assert result.empty

        # Missing majornums should yield nothing
        with self.assertWarnsRegex(UserWarning, "IPEDS"):
            filters = QueryFilters(majornums=2)
        result = self.engine.field_totals_by_grouping(
            grouping=Grouping.gender,
            taxonomy=FieldTaxonomy.ncses_field_group,
            query_filters=filters,
        )
        assert result.empty

    def test_group_query(self):
        # Test the basic group query
        one_stem = TaxonomyRollup(
            taxonomy_name=FieldTaxonomy.ncses_detailed_field_group,
            taxonomy_values=[NCSESDetailedFieldGroup.math],
        )
        with self.assertWarnsRegex(UserWarning, "IPEDS"):
            filters = QueryFilters()

        index_cols = [
            "gender",
        ]
        value_cols = [
            "rollup_degrees_within_gender",
            "rollup_degrees_total",
            "uni_degrees_within_gender",
            "uni_degrees_total",
        ]

        # One major in STEM = 3 men/women per uni in each group
        # Two unis => double
        # Two years => double again
        one_stem_gender_subtotals = np.array([3, 6, 9, 18]) * 2 * 2
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[[gender] + one_stem_gender_subtotals.tolist() for gender in self.genders],
        ).set_index(index_cols)
        result = self.engine.rollup_by_grouping(
            grouping=Grouping.gender, rollup=one_stem, query_filters=filters
        )
        self._check_result(result, expected)

        # Defining both math + psych as stem should change results
        two_stem = TaxonomyRollup(
            taxonomy_name=FieldTaxonomy.ncses_detailed_field_group,
            taxonomy_values=[NCSESDetailedFieldGroup.math, NCSESDetailedFieldGroup.psych],
        )
        # Two majors in STEM = 6 men/women per group per uni
        # Two unis => double
        # Two years => double again
        two_stem_gender_subtotals = np.array([6, 12, 9, 18]) * 2 * 2
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[[gender] + two_stem_gender_subtotals.tolist() for gender in self.genders],
        ).set_index(index_cols)
        result = self.engine.rollup_by_grouping(
            grouping=Grouping.gender, rollup=two_stem, query_filters=filters
        )
        self._check_result(result, expected)

        # Test grouping by race
        index_cols = [
            "race_ethnicity",
        ]
        value_cols = [
            "rollup_degrees_within_race_ethnicity",
            "rollup_degrees_total",
            "uni_degrees_within_race_ethnicity",
            "uni_degrees_total",
        ]
        # One major in STEM = 2 people per group per uni
        # Two unis => double
        # Two years => double again
        one_stem_re_subtotals = np.array([2, 6, 6, 18]) * 2 * 2
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[[race] + one_stem_re_subtotals.tolist() for race in self.race_ethnicities],
        ).set_index(index_cols)
        result = self.engine.rollup_by_grouping(
            grouping=Grouping.race_ethnicity, rollup=one_stem, query_filters=filters
        )
        self._check_result(result, expected)

        # Two majors in STEM = 2 * 2 = 4 people per group per uni
        # Two unis => double
        # Two years => double again
        two_stem_re_subtotals = np.array([4, 12, 6, 18]) * 2 * 2
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[[race] + two_stem_re_subtotals.tolist() for race in self.race_ethnicities],
        ).set_index(index_cols)
        result = self.engine.rollup_by_grouping(
            grouping=Grouping.race_ethnicity, rollup=two_stem, query_filters=filters
        )
        self._check_result(result, expected)

        # Test intersectional
        index_cols = [
            "race_ethnicity",
            "gender",
        ]
        value_cols = [
            "rollup_degrees_intersectional",
            "rollup_degrees_total",
            "uni_degrees_intersectional",
            "uni_degrees_total",
        ]

        # Splitting by gender+race with one major means we expect one person per sub-group
        # and 3 people within that sub-group overall at each uni
        # Two unis => double
        # Two years => double again
        subtotals = np.array([1, 6, 3, 18]) * 2 * 2
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[
                [race, gender] + subtotals.tolist()
                for race in self.race_ethnicities
                for gender in self.genders
            ],
        ).set_index(index_cols)
        result = self.engine.rollup_by_grouping(
            grouping=Grouping.intersectional, rollup=one_stem, query_filters=filters
        )
        self._check_result(result, expected)

        # Test by year
        # One major in STEM = 3 men/women per uni in each group
        # Two unis => double
        # one per year
        index_cols = [
            "gender",
            "year",
        ]
        value_cols = [
            "rollup_degrees_within_gender",
            "rollup_degrees_total",
            "uni_degrees_within_gender",
            "uni_degrees_total",
        ]
        one_stem_gender_subtotals = np.array([3, 6, 9, 18]) * 2 * 1
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[
                [gender, year] + one_stem_gender_subtotals.tolist()
                for gender in self.genders
                for year in self.years
            ],
        ).set_index(index_cols)
        result = self.engine.rollup_by_grouping(
            grouping=Grouping.gender, rollup=one_stem, query_filters=filters, by_year=True
        )
        self._check_result(result, expected)

        # test unitid filter
        # One major in STEM = 3 men/women per uni in each group
        # One uni only
        # one each year
        one_stem_gender_subtotals = np.array([3, 6, 9, 18]) * 1 * 1
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[
                [gender, year] + one_stem_gender_subtotals.tolist()
                for gender in self.genders
                for year in self.years
            ],
        ).set_index(index_cols)
        result = self.engine.rollup_by_grouping(
            grouping=Grouping.gender,
            rollup=one_stem,
            query_filters=filters,
            by_year=True,
            filter_unitids=[1],
        )
        self._check_result(result, expected)

    def test_group_fields_query(self):
        with self.assertWarnsRegex(UserWarning, "IPEDS"):
            filters = QueryFilters()
        index_cols = [
            "ncses_detailed_field_group",
            "gender",
        ]
        value_cols = [
            "field_degrees_within_gender",
            "field_degrees_total",
            "uni_degrees_within_gender",
            "uni_degrees_total",
        ]
        # 3 person per major per gender
        # 6 people per major total
        # 9 people per uni degree per gender
        # 18 people per uni degree total
        # 2 unis => double
        # 2 years => double
        subtotals = np.array([3, 6, 9, 18]) * 2 * 2
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[
                [dfg, gender] + subtotals.tolist() for dfg in self.dfgs for gender in self.genders
            ],
        ).set_index(index_cols)
        result = self.engine.field_totals_by_grouping(
            grouping=Grouping.gender,
            taxonomy=FieldTaxonomy.ncses_detailed_field_group,
            query_filters=filters,
        )
        self._check_result(result, expected)

        # Test race
        index_cols = [
            "ncses_detailed_field_group",
            "race_ethnicity",
        ]
        value_cols = [
            "field_degrees_within_race_ethnicity",
            "field_degrees_total",
            "uni_degrees_within_race_ethnicity",
            "uni_degrees_total",
        ]
        # 2 peple per major per race
        # 6 people per race subtotal
        # 6 people per uni per race
        # 18 people per uni total
        # 2 unis => double
        # 2 years => double
        subtotals = np.array([2, 6, 6, 18]) * 2 * 2
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[
                [dfg, race] + subtotals.tolist()
                for dfg in self.dfgs
                for race in self.race_ethnicities
            ],
        ).set_index(index_cols)
        result = self.engine.field_totals_by_grouping(
            grouping=Grouping.race_ethnicity,
            taxonomy=FieldTaxonomy.ncses_detailed_field_group,
            query_filters=filters,
        )
        self._check_result(result, expected)

        # Test by year
        index_cols = [
            "ncses_detailed_field_group",
            "race_ethnicity",
            "year",
        ]
        value_cols = [
            "field_degrees_within_race_ethnicity",
            "field_degrees_total",
            "uni_degrees_within_race_ethnicity",
            "uni_degrees_total",
        ]
        # 2 peple per major per race
        # 6 people per race subtotal
        # 6 people per uni per race
        # 18 people per uni total
        # 2 unis => double
        # each year
        subtotals = np.array([2, 6, 6, 18]) * 2 * 1
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[
                [dfg, race, year] + subtotals.tolist()
                for dfg in self.dfgs
                for race in self.race_ethnicities
                for year in self.years
            ],
        ).set_index(index_cols)
        result = self.engine.field_totals_by_grouping(
            grouping=Grouping.race_ethnicity,
            taxonomy=FieldTaxonomy.ncses_detailed_field_group,
            query_filters=filters,
            by_year=True,
        )
        self._check_result(result, expected)

        # Intersectional
        index_cols = [
            "ncses_detailed_field_group",
            "race_ethnicity",
            "gender",
        ]
        value_cols = [
            "field_degrees_intersectional",
            "field_degrees_total",
            "uni_degrees_intersectional",
            "uni_degrees_total",
        ]
        # 1 person per major per race+gender
        # 6 people per major per total
        # 3 people per uni per race+gender
        # 18 people per uni total
        # 2 unis => double
        # 2 years => double
        subtotals = np.array([1, 6, 3, 18]) * 2 * 2
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[
                [field, race, gender] + subtotals.tolist()
                for field in self.dfgs
                for race in self.race_ethnicities
                for gender in self.genders
            ],
        ).set_index(index_cols)
        result = self.engine.field_totals_by_grouping(
            grouping=Grouping.intersectional,
            taxonomy=FieldTaxonomy.ncses_detailed_field_group,
            query_filters=filters,
        )
        self._check_result(result, expected)

        # Test unitid filter
        # 1 person per major per race+gender
        # 6 people per major per total
        # 3 people per uni per race+gender
        # 18 people per uni total
        # 1 uni
        # 2 years => double
        subtotals = np.array([1, 6, 3, 18]) * 1 * 2
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[
                [field, race, gender] + subtotals.tolist()
                for field in self.dfgs
                for race in self.race_ethnicities
                for gender in self.genders
            ],
        ).set_index(index_cols)
        result = self.engine.field_totals_by_grouping(
            grouping=Grouping.intersectional,
            taxonomy=FieldTaxonomy.ncses_detailed_field_group,
            query_filters=filters,
            filter_unitids=[2],
        )
        self._check_result(result, expected)

    def test_uni_query(self):
        # Test gender
        with self.assertWarnsRegex(UserWarning, "IPEDS"):
            filters = QueryFilters()
        fields_agg = TaxonomyRollup(
            taxonomy_name=FieldTaxonomy.ncses_sci_group, taxonomy_values=NCSESSciGroup.sci
        )
        index_cols = [
            "institution_name",
            "unitid",
            "gender",
        ]
        value_cols = [
            "rollup_degrees_within_gender",
            "rollup_degrees_total",
            "uni_degrees_within_gender",
            "uni_degrees_total",
        ]
        # 3 people per major within gender per uni
        # 2 timesteps
        subtotals = np.array([3, 6, 9, 18]) * 2
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[
                [self.names[unitid], unitid, gender] + subtotals.tolist()
                for unitid in self.unitids
                for gender in self.genders
            ],
        ).set_index(index_cols)
        result = self.engine.uni_rollup_by_grouping(
            grouping=Grouping.gender, rollup=fields_agg, query_filters=filters
        )
        self._check_result(result, expected)

        # By year
        index_cols = [
            "institution_name",
            "unitid",
            "gender",
            "year",
        ]
        value_cols = [
            "rollup_degrees_within_gender",
            "rollup_degrees_total",
            "uni_degrees_within_gender",
            "uni_degrees_total",
        ]
        # 3 people per major within gender per uni
        subtotals = np.array([3, 6, 9, 18])
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[
                [self.names[unitid], unitid, gender, year] + subtotals.tolist()
                for unitid in self.unitids
                for gender in self.genders
                for year in self.years
            ],
        ).set_index(index_cols)
        result = self.engine.uni_rollup_by_grouping(
            grouping=Grouping.gender, rollup=fields_agg, query_filters=filters, by_year=True
        )
        self._check_result(result, expected)

        # Test race/ethn
        index_cols = [
            "institution_name",
            "unitid",
            "race_ethnicity",
        ]
        value_cols = [
            "rollup_degrees_within_race_ethnicity",
            "rollup_degrees_total",
            "uni_degrees_within_race_ethnicity",
            "uni_degrees_total",
        ]
        # 2 people per major per race/ethn per uni
        # 1 STEM major of 3 total majors
        # 2 timesteps
        subtotals = np.array([2, 6, 6, 18]) * 2
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[
                [self.names[unitid], unitid, race_ethn] + subtotals.tolist()
                for unitid in self.unitids
                for race_ethn in self.race_ethnicities
            ],
        ).set_index(index_cols)
        result = self.engine.uni_rollup_by_grouping(
            grouping=Grouping.race_ethnicity, rollup=fields_agg, query_filters=filters
        )
        self._check_result(result, expected)

        # Test intersectional
        index_cols = [
            "institution_name",
            "unitid",
            "race_ethnicity",
            "gender",
        ]
        value_cols = [
            "rollup_degrees_intersectional",
            "rollup_degrees_total",
            "uni_degrees_intersectional",
            "uni_degrees_total",
        ]
        # 2 people per major per race/ethn per uni
        # 1 STEM major of 3 total majors
        # 2 timesteps
        subtotals = np.array([1, 6, 3, 18]) * 2
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[
                [self.names[unitid], unitid, race_ethn, gender] + subtotals.tolist()
                for unitid in self.unitids
                for race_ethn in self.race_ethnicities
                for gender in self.genders
            ],
        ).set_index(index_cols)
        result = self.engine.uni_rollup_by_grouping(
            grouping=Grouping.intersectional, rollup=fields_agg, query_filters=filters
        )
        self._check_result(result, expected)

        # Test unitid filter
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[
                [self.names[unitid], unitid, race_ethn, gender] + subtotals.tolist()
                for unitid in [1]
                for race_ethn in self.race_ethnicities
                for gender in self.genders
            ],
        ).set_index(index_cols)
        result = self.engine.uni_rollup_by_grouping(
            grouping=Grouping.intersectional,
            rollup=fields_agg,
            query_filters=filters,
            filter_unitids=[1],
        )
        self._check_result(result, expected)

    def test_uni_fields_query(self):
        # Test gender
        with self.assertWarnsRegex(UserWarning, "IPEDS"):
            filters = QueryFilters()
        index_cols = [
            "institution_name",
            "unitid",
            "ncses_detailed_field_group",
            "gender",
        ]
        value_cols = [
            "field_degrees_within_gender",
            "field_degrees_total",
            "uni_degrees_within_gender",
            "uni_degrees_total",
        ]
        # 3 people per major within gender per uni
        # 2 timesteps
        subtotals = np.array([3, 6, 9, 18]) * 2
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[
                [self.names[unitid], unitid, dfg, gender] + subtotals.tolist()
                for unitid in self.unitids
                for dfg in self.dfgs
                for gender in self.genders
            ],
        ).set_index(index_cols)
        result = self.engine.uni_field_totals_by_grouping(
            grouping=Grouping.gender,
            taxonomy=FieldTaxonomy.ncses_detailed_field_group,
            query_filters=filters,
        )
        self._check_result(result, expected)

        # Test by year
        index_cols = [
            "institution_name",
            "unitid",
            "ncses_detailed_field_group",
            "gender",
            "year",
        ]
        # 3 people per major within gender per uni
        subtotals = np.array([3, 6, 9, 18])
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[
                [self.names[unitid], unitid, dfg, gender, year] + subtotals.tolist()
                for unitid in self.unitids
                for dfg in self.dfgs
                for gender in self.genders
                for year in self.years
            ],
        ).set_index(index_cols)
        result = self.engine.uni_field_totals_by_grouping(
            grouping=Grouping.gender,
            taxonomy=FieldTaxonomy.ncses_detailed_field_group,
            query_filters=filters,
            by_year=True,
        )
        self._check_result(result, expected)

        # Test race/ethn
        index_cols = [
            "institution_name",
            "unitid",
            "ncses_detailed_field_group",
            "race_ethnicity",
        ]
        value_cols = [
            "field_degrees_within_race_ethnicity",
            "field_degrees_total",
            "uni_degrees_within_race_ethnicity",
            "uni_degrees_total",
        ]
        # 2 people per major per race/ethn per uni
        # 1 STEM major of 3 total majors
        # 2 timesteps
        subtotals = np.array([2, 6, 6, 18]) * 2
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[
                [self.names[unitid], unitid, dfg, race_ethn] + subtotals.tolist()
                for unitid in self.unitids
                for dfg in self.dfgs
                for race_ethn in self.race_ethnicities
            ],
        ).set_index(index_cols)
        result = self.engine.uni_field_totals_by_grouping(
            grouping=Grouping.race_ethnicity,
            taxonomy=FieldTaxonomy.ncses_detailed_field_group,
            query_filters=filters,
        )
        self._check_result(result, expected)

        # Test intersectional
        index_cols = [
            "institution_name",
            "unitid",
            "ncses_detailed_field_group",
            "race_ethnicity",
            "gender",
        ]
        value_cols = [
            "field_degrees_intersectional",
            "field_degrees_total",
            "uni_degrees_intersectional",
            "uni_degrees_total",
        ]
        # 2 people per major per race/ethn per uni
        # 1 STEM major of 3 total majors
        # 2 timesteps
        subtotals = np.array([1, 6, 3, 18]) * 2
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[
                [self.names[unitid], unitid, dfg, race_ethn, gender] + subtotals.tolist()
                for unitid in self.unitids
                for dfg in self.dfgs
                for race_ethn in self.race_ethnicities
                for gender in self.genders
            ],
        ).set_index(index_cols)
        result = self.engine.uni_field_totals_by_grouping(
            grouping=Grouping.intersectional,
            taxonomy=FieldTaxonomy.ncses_detailed_field_group,
            query_filters=filters,
        )
        self._check_result(result, expected)

        # Test intersectional by year with filtered fields
        index_cols = [
            "institution_name",
            "unitid",
            "ncses_detailed_field_group",
            "race_ethnicity",
            "gender",
            "year",
        ]
        value_cols = [
            "field_degrees_intersectional",
            "field_degrees_total",
            "uni_degrees_intersectional",
            "uni_degrees_total",
        ]
        # 2 people per major per race/ethn per uni
        # 1 STEM major of 3 total majors
        # 2 timesteps
        subtotals = np.array([1, 6, 3, 18])
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[
                [self.names[unitid], unitid, dfg, race_ethn, gender, year] + subtotals.tolist()
                for unitid in self.unitids
                for dfg in self.dfgs[:2]
                for race_ethn in self.race_ethnicities
                for gender in self.genders
                for year in self.years
            ],
        ).set_index(index_cols)
        result = self.engine.uni_field_totals_by_grouping(
            grouping=Grouping.intersectional,
            taxonomy=FieldTaxonomy.ncses_detailed_field_group,
            taxonomy_values=self.dfgs[:2],
            query_filters=filters,
            by_year=True,
        )
        self._check_result(result, expected)

        # Test unitid filter
        subtotals = np.array([1, 6, 3, 18])
        expected = pd.DataFrame(
            columns=index_cols + value_cols,
            data=[
                [self.names[unitid], unitid, dfg, race_ethn, gender, year] + subtotals.tolist()
                for unitid in [2]
                for dfg in self.dfgs
                for race_ethn in self.race_ethnicities
                for gender in self.genders
                for year in self.years
            ],
        ).set_index(index_cols)
        result = self.engine.uni_field_totals_by_grouping(
            grouping=Grouping.intersectional,
            taxonomy=FieldTaxonomy.ncses_detailed_field_group,
            taxonomy_values=self.dfgs,
            query_filters=filters,
            by_year=True,
            filter_unitids=[2],
        )
        self._check_result(result, expected)

    def test_rel_rate(self):
        with self.assertWarnsRegex(UserWarning, "IPEDS"):
            filters = QueryFilters()
        for grouping in Grouping:
            for taxonomy in FieldTaxonomy:
                for yearly in (True, False):
                    if taxonomy == FieldTaxonomy.original_cip:
                        continue
                    result = self.engine.field_totals_by_grouping(
                        grouping=grouping,
                        taxonomy=taxonomy,
                        query_filters=filters,
                        by_year=yearly,
                        rel_rate=True,
                    )
                    assert "rel_rate" in result.columns
                    assert "excess_degrees_from_parity" in result.columns

    def test_effect_size(self):
        with self.assertWarnsRegex(UserWarning, "IPEDS"):
            filters = QueryFilters()
        taxonomies = [
            FieldTaxonomy.ncses_detailed_field_group,
            FieldTaxonomy.dhs_stem,
            FieldTaxonomy.cip,
        ]
        values = [self.dfgs, self.dhs_stems, self.cips]

        for grouping in Grouping:
            for taxonomy, vals in zip(taxonomies, values):
                for yearly in (True, False):
                    rollup = TaxonomyRollup(taxonomy_name=taxonomy.value, taxonomy_values=vals)
                    result = self.engine.uni_rollup_by_grouping(
                        grouping=grouping,
                        rollup=rollup,
                        query_filters=filters,
                        by_year=yearly,
                        rel_rate=True,
                        effect_size=True,
                    )
                    assert "excess_degrees_from_median_expected" in result.columns

        for grouping in Grouping:
            for taxonomy in FieldTaxonomy:
                if taxonomy == FieldTaxonomy.original_cip:
                    continue
                for yearly in (True, False):
                    result = self.engine.uni_field_totals_by_grouping(
                        grouping=grouping,
                        taxonomy=taxonomy,
                        query_filters=filters,
                        by_year=yearly,
                        rel_rate=True,
                        effect_size=True,
                    )
                    assert "excess_degrees_from_median_expected" in result.columns
