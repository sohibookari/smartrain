import numpy as np
from pandas import Series


def series_to_ndarray(s: Series):
    return np.array(s).reshape(-1, 1)
