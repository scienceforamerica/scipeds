import warnings
from typing import Any, List

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from typing_extensions import Annotated

from scipeds import constants
from scipeds.data.enums import AwardLevel, FieldTaxonomy, RaceEthn, Geo
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
        # check if the inputted value has a "tolist" or "to_list" method
        # and call the appropriate method if so
        if hasattr(v, "tolist"):
            v = v.tolist()
        elif hasattr(v, "to_list"):
            v = v.to_list()
        elif not isinstance(v, list):
            v = [v]
        return v


class BaseQueryFilters(BaseModel):
    """A pydantic model with options for how to filter the baseline data prior to any aggregation.

    This is the base model, with filters that apply to all IPEDS datasets.

    The model will handle data validation for the input values by ensuring:

    - The years are in range and the range is nonzero
    - The race/ethnicity and degree values (if provided) are valid
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
        
        return self
    
class CompletionsQueryFilters(BaseQueryFilters):

    start_year: Annotated[int, Field(ge=constants.FALL_SURVEYS_START_YEAR, le=constants.FALL_SURVEYS_END_YEAR)] = (
        constants.START_YEAR
    )
    """The start year of the time window to aggregate over, inclusive"""

    end_year: Annotated[int, Field(ge=constants.FALL_SURVEYS_START_YEAR, le=constants.FALL_SURVEYS_END_YEAR)] = (
        constants.END_YEAR
    )
    """The end year of the time window to aggregate over, inclusive"""


    race_ethns: List[RaceEthn] = list(RaceEthn)
    """ Which race/ethnicity groups to include (default: all)"""

    award_levels: List[AwardLevel] = list(AwardLevel)
    """Which degrees / award levels to include (default: all)"""

    majornums: List[Annotated[int, Field(ge=1, le=2)]] = [1, 2]
    """Which major numbers to include (default: both first and second majors)"""

    @model_validator(mode="after")
    def check_year_range_and_race_ethnicities(self):
        # Warn if time range overlaps span where racial categorization changes
        if self.start_year <= 2010 and self.end_year >= 2011:
            warnings.warn(
                "IPEDS award level coding and race and ethnicity coding changed "
                "between 2010 and 2011 datasets. For more details, see "
                "https://docs.scipeds.org/faq/#what-data-is-currently-included-in-scipeds"
            )
        if (self.start_year < 1995 or self.end_year < 1995) and self.race_ethns != [
            RaceEthn.unknown
        ]:
            warnings.warn(
                "Race/ethnicity data is not available before 1995. All race/ethnicities "
                "prior to 1995 are treated as 'unknown'."
            )
        return self

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

# Backward compatibility alias with deprecation warning
class QueryFilters(CompletionsQueryFilters):
    def __init__(self, **kwargs):
        warnings.warn(
            "QueryFilters is deprecated and will be removed in a future version. "
            "Use CompletionsQueryFilters instead.",
            FutureWarning,
            stacklevel=2
        )
        super().__init__(**kwargs)

class FallEnrollmentQueryFilters(BaseQueryFilters):

    start_year: Annotated[int, Field(ge=constants.SPRING_SURVEYS_START_YEAR, le=constants.SPRING_SURVEYS_END_YEAR)] = (
        constants.START_YEAR
    )
    """The start year of the time window to aggregate over, inclusive"""

    end_year: Annotated[int, Field(ge=constants.SPRING_SURVEYS_START_YEAR, le=constants.SPRING_SURVEYS_END_YEAR)] = (
        constants.END_YEAR
    )
    """The end year of the time window to aggregate over, inclusive"""

    geos: List[Geo] = list(Geo)
    """ Which geographic locations to include (default: all)"""
    
    @field_validator("geos", mode="before")
    @classmethod
    def geo_valid(cls, v: Any):
        return validate_and_listify(v, Geo)