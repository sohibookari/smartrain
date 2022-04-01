class Remover:
    def __init__(self, data):
        self._data = data
        self._selected = set()

    def select_by_colvalue(self, col, value):
        index = self._data[self._data[col] == value].index
        self._selected.update(set(index))
        return self

    def remove(self):
        return self._data.drop(index=self._selected)
