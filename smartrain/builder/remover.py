from logging import Logger

import pandas as pd
import smartrain.context as ctx


class Remover:
    def __init__(self, data):
        self.logger: Logger = ctx.get('logger')
        self._data = data
        self._selected = set()

    def select_by_colvalue(self, col, value):
        index = self._data[self._data[col] == value].index
        self._selected.update(set(index))
        return self

    def select_by_null(self, col):
        index = self._data[self._data[col].isna()].index
        self._selected.update(set(index))
        return self

    def remove(self) -> pd.DataFrame:
        return self._data.drop(index=self._selected)
