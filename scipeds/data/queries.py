import warnings
from typing import Any, List

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from typing_extensions import Annotated

from scipeds import constants
from scipeds.data.enums import AwardLevel, FieldTaxonomy, RaceEthn
from scipeds.utils import validate_and_listify


class TaxonomyRollup(BaseModel):
    """Defining values within a taxonomy to include in an aggregation"""

    model_config = ConfigDict(extra="forbid")

    taxonomy_name: FieldTaxonomy
    """The taxonomy within which to aggregate over values"""

    taxonomy_values: List[Any]
    """The values in the taxonomy to include in aggregation"""

    @field_validator("taxonomy_values", mode="before")
    @classmethod
    def values_valid_list(cls, v: Any):
        if not isinstance(v, list):
            v = [v]
        return v


class QueryFilters(BaseModel):
    """A pydantic model with options for how to filter the baseline data prior to any aggregation.

    The model will handle data validation for the input values by ensuring:

    - The years are in range and the range is nonzero
    - The race/ethnicity and degree values provided are valid
    - Things that ought to be lists are lists
    """

    model_config = ConfigDict(extra="forbid")

    start_year: Annotated[int, Field(ge=constants.START_YEAR, le=constants.END_YEAR)] = (
        constants.START_YEAR
    )
    """The start year of the time window to aggregate over, inclusive"""

    end_year: Annotated[int, Field(ge=constants.START_YEAR, le=constants.END_YEAR)] = (
        constants.END_YEAR
    )
    """The end year of the time window to aggregate over, inclusive"""

    race_ethns: List[RaceEthn] = list(RaceEthn)
    """ Which race/ethnicity groups to include (default: all)"""
    # TODO: will adding "Total" to this break things or not?
    # It won't break and it shouldn't return erroneous data either, bc there are no years that have both "Total" as a race/ethnicity and also the individual ones

    award_levels: List[AwardLevel] = list(AwardLevel)
    """Which degrees / award levels to include (default: all)"""

    majornums: List[Annotated[int, Field(ge=1, le=2)]] = [1, 2]
    """Which major numbers to include (default: both first and second majors)"""

    @field_validator("race_ethns", mode="before")
    @classmethod
    def race_ethn_valid(cls, v: Any):
        # Validate provided race/ethnicity values, turn into a list
        return validate_and_listify(v, RaceEthn)

    @field_validator("award_levels", mode="before")
    @classmethod
    def award_valid(cls, v: Any):
        # Validate provided degree values, turn into a list
        return validate_and_listify(v, AwardLevel)

    @field_validator("majornums", mode="before")
    @classmethod
    def majornum_valid(cls, v: Any):
        # Validate provided majornum values, turn into a list
        if isinstance(v, int):
            v = [v]
        return v

    @model_validator(mode="after")
    def check_year_range(self):
        """Validate year values

        Warns:
            UserWarning: If time range overlaps significant changes in IPEDS schema or values

        Raises:
            ValueError: If provided start_year is after end_year
        """
        if self.start_year > self.end_year:
            raise ValueError("Start year must be less than or equal to end year")
        # Warn if time range overlaps span where racial categorization changes
        if self.start_year <= 2010 and self.end_year >= 2011:
            warnings.warn(
                "IPEDS award level coding and race and ethnicity coding changed "
                "between 2010 and 2011 datasets. For more details, see "
                "https://nces.ed.gov/ipeds/report-your-data/race-ethnicity-reporting-changes"
            )
        return self
