import json
import os.path
import pandas as pd
import yaml

from .base import Reader


class FileReader(Reader):
    def __init__(self, file, path):
        super().__init__()
        self._file = file
        self._path = path

    def read(self) -> pd.DataFrame:
        return pd.read_csv(self._abs(), index_col=0)

    def read_pkl(self) -> pd.DataFrame:
        return pd.read_pickle(self._abs())

    def _abs(self) -> str:
        return self._path + self._file

    def exists(self) -> bool:
        return os.path.exists(self._abs())


class JSONReader(FileReader):

    def __init__(self, file, path):
        super().__init__(file, path)

    def read(self) -> list:
        with open(self._abs(), mode='r') as f:
            raw = f.read()
        return json.loads(raw)


class YAMLReader(FileReader):

    def __init__(self, file, path):
        super().__init__(file, path)

    def read(self) -> dict:
        with open(self._abs()) as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
        return config
