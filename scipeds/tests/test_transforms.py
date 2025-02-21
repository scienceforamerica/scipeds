import numpy as np
from numpy.testing import assert_array_almost_equal

from scipeds.utils import (
    bounded_ratio_transform,
    forward_fold_transform,
    inverse_bounded_ratio_transform,
    inverse_fold_transform,
)


def test_forward_fold_transform():
    """Test the forward fold transform

    Expected behavior
        # 1 -> 0      # Parity
        # 0.5 -> -1   # 2x under-represented
        # 3 -> 2      # 3x over-represented
        # 0 -> -inf   # completely underrepresented
        # inf -> inf  # completely overrepresented
    """
    values = np.array([1, 0.5, 3, 0, np.inf])
    expected = np.array([0, -1, 2, -np.inf, np.inf])
    transformed = forward_fold_transform(values)
    assert_array_almost_equal(expected, transformed)


def test_inverse_fold_transform():
    """Test the inverse fold transform

    Expected behavior
        # 0 -> 1      # Parity
        # -1 -> 0.5   # 2x under-represented
        # 2 -> 3      # 3x over-represented
        # -inf -> 0   # completely underrepresented
        # inf -> inf  # completely overrepresented
    """
    values = np.array([0, -1, 2, -np.inf, np.inf])
    expected = np.array([1, 0.5, 3, 0, np.inf])
    transformed = inverse_fold_transform(values)
    assert_array_almost_equal(expected, transformed)


def test_bounded_transform():
    """Test the bounded ratio transform

    Expected behavior
        1 -> 0        # Parity
        0.5 -> -0.5   # 2x under-represented
        3 -> 0.66666  # 3x over-represented
        0 -> -1       # completely underrepresented
        inf -> 1      # completely overrepresented
    """
    values = np.array([1, 0.5, 3, 0, np.inf])
    expected = np.array([0, -0.5, 2 / 3, -1, 1])
    transformed = bounded_ratio_transform(values)
    assert_array_almost_equal(expected, transformed)


def test_inverse_bounded_transform():
    """Test the bounded ratio transform

    Expected behavior
        1 -> 0        # Parity
        0.5 -> -0.5   # 2x under-represented
        3 -> 0.66666  # 3x over-represented
        0 -> -1       # completely underrepresented
        inf -> 1      # completely overrepresented
    """
    values = np.array([0, -0.5, 2 / 3, -1, 1])
    expected = np.array([1, 0.5, 3, 0, np.inf])
    transformed = inverse_bounded_ratio_transform(values)
    assert_array_almost_equal(expected, transformed)
