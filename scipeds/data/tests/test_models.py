import unittest

from pydantic import ValidationError

from scipeds import constants
from scipeds.data.enums import AwardLevel, FieldTaxonomy, Grouping, RaceEthn
from scipeds.data.queries import QueryFilters, TaxonomyRollup


class QueryFilterTests(unittest.TestCase):
    def test_years(self):
        # Bad year ranges fail validation
        with self.assertRaises(ValidationError):
            QueryFilters(start_year=2000, end_year=1999)

        # Race/ethnicity change year span raises warning
        with self.assertWarnsRegex(UserWarning, "IPEDS"):
            QueryFilters(start_year=2010, end_year=2011)

            # Default query filter produces default values
            qf = QueryFilters()
        assert qf.start_year == constants.START_YEAR
        assert qf.end_year == constants.END_YEAR

        # Adding values outside the bounds won't validate
        with self.assertRaises(ValidationError):
            QueryFilters(start_year=constants.START_YEAR - 1)
        with self.assertRaises(ValidationError):
            QueryFilters(end_year=constants.END_YEAR + 1)

    def test_race_ethns(self):
        # Adding singular values yields lists
        with self.assertWarnsRegex(UserWarning, "IPEDS"):
            qf = QueryFilters(race_ethns=RaceEthn.american_indian)
        assert qf.race_ethns == [RaceEthn.american_indian]

        # Correct strings are okay
        with self.assertWarnsRegex(UserWarning, "IPEDS"):
            qf = QueryFilters(race_ethns=[RaceEthn.american_indian])
        assert qf.race_ethns == [RaceEthn.american_indian]

        # Unidentified values fail
        with self.assertRaises(ValidationError):
            QueryFilters(race_ethns=["male"])

    def test_degrees(self):
        # Adding singular values yields lists
        with self.assertWarnsRegex(UserWarning, "IPEDS"):
            qf = QueryFilters(award_levels=AwardLevel.bachelors)
        assert qf.award_levels == [AwardLevel.bachelors]

        # Correct strings are okay
        with self.assertWarnsRegex(UserWarning, "IPEDS"):
            qf = QueryFilters(race_ethns=[RaceEthn.american_indian.value])
        assert qf.race_ethns == [RaceEthn.american_indian]

        # Unidentified values fail
        with self.assertRaises(ValidationError):
            QueryFilters(race_ethns=["male"])

    def test_majornum(self):
        # Value of 3 fails
        with self.assertRaises(ValidationError):
            QueryFilters(majornums=[3])


class FieldTaxonomyTests(unittest.TestCase):
    def test_fields(self):
        # Test valid column name
        sd = TaxonomyRollup(taxonomy_name="cipcode", taxonomy_values="12.3456")
        assert sd.taxonomy_name == FieldTaxonomy.original_cip
        assert sd.taxonomy_values == ["12.3456"]

        # Test invalid column name
        with self.assertRaises(ValidationError):
            TaxonomyRollup(taxonomy_name="not_a_valid_taxonomy", taxonomy_values=[1])


class GroupingTests(unittest.TestCase):
    def test_suffix(self):
        assert Grouping.intersectional.students_suffix == "students"
        assert Grouping.gender.students_suffix == ""
        assert Grouping.race_ethnicity.students_suffix == ""

    def test_label(self):
        assert Grouping.intersectional.label_suffix == "intersectional"
        assert Grouping.gender.label_suffix == "within_gender"
        assert Grouping.race_ethnicity.label_suffix == "within_race_ethnicity"

    def test_grouping_columns(self):
        assert Grouping.intersectional.grouping_columns == ["race_ethnicity", "gender"]
        assert Grouping.gender.grouping_columns == ["gender"]
        assert Grouping.race_ethnicity.grouping_columns == ["race_ethnicity"]
