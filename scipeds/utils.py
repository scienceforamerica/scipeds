from collections import namedtuple
from pathlib import Path
from typing import Any, Iterable, List, Union

import numpy as np
import numpy.typing as npt
import pandas as pd
from statsmodels.stats import proportion  # type: ignore

Rate = namedtuple("Rate", ["name", "numerator", "denominator"])


def forward_fold_transform(values: npt.NDArray | pd.Series) -> npt.NDArray:
    """Transform values from raw relative rate values to a linear fold scale.

    Relative rates are asymmetric around parity and on the half-open interval [0, +np.inf).
    This function transforms relative rates into a linear scale of the multiplicative value
    indicated by the rate so that the scale is symmetric around zero (parity) and on the interval
    (-np.inf, np.inf).

    Examples:
        1 -> 0  # Parity
        0.5 -> -1  # 2x under-represented
        3 -> 2  # 3x over-represented
        0 -> -inf # completely underrepresented
        inf -> inf # completely overrepresented

    Note that the numeric values on a linear fold scale differ from the labeled values by 1.
    """
    values = np.asarray(values).copy()
    null_idx = np.flatnonzero(values <= 0)
    values[null_idx] = np.nan
    transformed = np.array(list(map(lambda x: 1 - np.divide(1, x) if x <= 1 else x - 1, values)))
    transformed[null_idx] = -np.inf
    return transformed


def inverse_fold_transform(values: npt.NDArray | pd.Series) -> npt.NDArray:
    """Transform values from a linear fold scale to raw value"""
    return np.array(list(map(lambda x: -1 / (x - 1) if x < 0 else x + 1, values)))


def bounded_ratio_transform(values: npt.NDArray | pd.Series) -> npt.NDArray:
    """Transform values from raw relative rate values to a bounded ratio scale.

    Relative rates are asymmetric around parity and on the half-open interval [0, +np.inf).
    For certain kinds of clustering algorithms, it's useful for these to be on a symmetric,
    bounded interval.

    This function transforms the half-open interval to a fully closed interval [-1, 1]
    where 0 is parity, -1 is completely under-represented, and 1 is completely over-represented.

    Examples:
        1 -> 0  # Parity
        0.5 -> -0.5  # 2x under-represented
        3 -> 0.66666  # 3x over-represented
        0 -> -1 # completely underrepresented
        inf -> 1 # completely overrepresented
    """
    values = values.copy()
    transformed = np.array(list(map(lambda x: 1 - np.divide(1, x) if x >= 1 else x - 1, values)))
    return transformed


def inverse_bounded_ratio_transform(values: npt.NDArray | pd.Series) -> npt.NDArray:
    """Transform values from a bounded ratio scale to a raw relative rate.

    Examples:
        0 -> 1  # Parity
        -0.5 -> 0.5  # 2x under-represented
        2/3 -> 3  # 3x over-represented
        -1 -> 0 # completely underrepresented
        1 -> inf # completely overrepresented
    """
    values = values.copy()

    def _invert(x):
        if x <= 0:
            return x + 1
        if x < 1:
            return 1 / (1 - x)
        return np.inf

    transformed = np.array(list(map(_invert, values)))
    return transformed


def calculate_rel_rate(df: pd.DataFrame, subgroup: Rate, baseline: Rate) -> pd.DataFrame:
    """Calculate multiple kinds of relative rates given a subgroup and a baseline Rate"""
    df = df.copy()
    df[subgroup.name] = df[subgroup.numerator] / df[subgroup.denominator]
    df[baseline.name] = df[baseline.numerator] / df[baseline.denominator]
    df["rel_rate"] = df[subgroup.name] / df[baseline.name]
    df["log2_rel_rate"] = df["rel_rate"].apply(np.log2)
    df["fold_rel_rate"] = forward_fold_transform(df["rel_rate"])
    df["bounded_rel_rate"] = bounded_ratio_transform(df["rel_rate"])

    return df


def calculate_effect_size(
    df: pd.DataFrame, field_pct: Rate, uni_pct: Rate, group_cols: List[str]
) -> pd.DataFrame:
    """Compute the number of degrees expected if a university's relative rate in a given field
    were the same as the median relative rate in that field among all universities
    """
    df = df.copy()
    # Calculate distance from parity (and z-score)
    df["excess_degrees_from_parity"] = df[field_pct.numerator] - (
        df[uni_pct.name] * df[field_pct.denominator]
    )
    df[["excess_degrees_from_parity_zscore", "excess_degrees_from_parity_pvalue"]] = get_zscore(
        df, field_pct, uni_pct
    )
    df["median_rel_rate_uni"] = df.groupby(group_cols, observed=True).rel_rate.transform(
        lambda x: x.median()
    )
    df["median_rel_rate_expected_degrees"] = (
        df[field_pct.denominator] * df[uni_pct.name] * df["median_rel_rate_uni"]
    )
    # Avoid the scenario where the number of expected degrees is larger than the field size
    df["median_rel_rate_expected_degrees"] = df[
        ["median_rel_rate_expected_degrees", field_pct.denominator]
    ].min(axis=1)
    df["excess_degrees_from_median_expected"] = (
        df[field_pct.numerator] - df["median_rel_rate_expected_degrees"]
    )
    expected_pct = Rate(
        "median_rel_rate_expected_pct", "median_rel_rate_expected_degrees", field_pct.denominator
    )
    df[
        [
            "excess_degrees_from_median_expected_zscore",
            "excess_degrees_from_median_expected_pvalue",
        ]
    ] = get_zscore(df, field_pct, expected_pct)
    return df


def get_zscore(df: pd.DataFrame, subgroup: Rate, baseline: Rate) -> pd.Series:
    """Calculate test statistic using 2-sample test for proportions"""
    group_count, group_obs = df[subgroup.numerator], df[subgroup.denominator]
    base_count, base_obs = df[baseline.numerator], df[baseline.denominator]
    with np.errstate(divide="ignore", invalid="ignore"):
        res = proportion.test_proportions_2indep(
            group_count,
            group_obs,
            base_count,
            base_obs,
            method="agresti-caffo",
            compare="diff",
        )
    return pd.Series([res.statistic, res.pvalue])


def convert_to_series(s: Union[str, Iterable]) -> pd.Series:
    """Convert input to a pandas Series

    Args:
        s (Union[str, List[str], pd.Series]): Desired input string(s)
    """
    if isinstance(s, str):
        s = [s]
    if isinstance(s, list) or isinstance(s, np.ndarray):
        s = pd.Series(s)
    if isinstance(s, pd.Index):
        s = s.to_series()
    if isinstance(s, pd.Series):
        return s.copy()
    raise ValueError(f"Provided value {s} must be str, List[str], pd.Series, or pd.Index")


def clean_name(s: str):
    """Cleans a column name by removing punctuation and replacing whitespace with underscores"""
    s = s.replace("'", "")
    for symbol in "-:,()/":
        s = s.replace(symbol, " ")
    return "_".join(s.lower().split())


def validate_and_listify(input: Any, cls: Iterable):
    """Convenience function to validate values are members of an Enum and ensure list returned"""
    if isinstance(input, str):
        input = [input]
    elif not isinstance(input, list):
        input = list(input)
    allowed_values = set(cls)
    for value in input:
        if value not in allowed_values:
            raise ValueError(f"Value {value} not a valid member of {cls}")
    return input


def read_excel_with_lowercase_sheets(filename: Path, sheet_name: str):
    """Convenience function to read an Excel sheet, convert its sheet names to
    lowercase, and then return the requested sheet name
    """
    all_sheets = pd.read_excel(filename, sheet_name=None)
    all_sheets = {name.lower(): df for name, df in all_sheets.items()}
    return all_sheets[sheet_name.lower()]
