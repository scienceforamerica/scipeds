import unittest

from pydantic import ValidationError

from scipeds import constants
from scipeds.data.enums import AwardLevel, FieldTaxonomy, RaceEthn
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
