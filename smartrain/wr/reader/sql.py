import threading
from typing import Any

import pandas as pd
import sqlalchemy.engine

from sqlalchemy import create_engine
from .base import Reader
from ..cache import SqlCache
from ..writer import FileWriter


class SqlReader(Reader):
    def __init__(self, conn=None):
        super().__init__()
        self._connection = conn

    def _conn(self):
        if self._connection:
            return self._connection
        else:
            engine = create_engine(self.config.DB_URL)
            return engine.connect()

    def _read(self, query):
        return pd.read_sql(sql=query, con=self._conn())

    def read(self, query, alias=None) -> pd.DataFrame:
        def generator(path, filename):
            df = self._read(query)
            FileWriter(path=path, file=filename).pkl(data=df)
            return df

        length = self._check_len(query)
        cache = SqlCache()
        return cache.fetch(query, length, generator, alias)

    def reads(self, query=(), keys=()) -> map:
        threads = []

        for i, q in enumerate(query):
            args = (q, keys[i]) if keys else [q]
            t = ReadThread(target=self.read, args=args)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        return map(lambda th: th.result(), threads)

    def _check_len(self, query: str):
        sub = query[query.index('FROM'):]
        result = self._conn().execute('SELECT COUNT(*) ' + sub).fetchone()
        return result[0]


class DictReader(SqlReader):
    def __init__(self, view_name, conn=None):
        super().__init__(conn)
        self._query_dict = self.config.VIEWS[view_name]

    def read_view(self, view):
        return self.read(self._query_dict[view], alias=view)

    def read_threading(self) -> list:
        result = []
        keys = self._query_dict.keys()
        results = self.reads(query=self._query_dict.values(), keys=list(keys))

        for name, res in zip(keys, results):
            result.append((name, res))

        return result

    def read_no_threading(self) -> list:
        result = []
        keys = self._query_dict.keys()

        for key in keys:
            result.append((key, self.read_view(key)))

        return result


class ReadThread(threading.Thread):
    def __init__(self, target=None, args=(), kwargs=None):
        super().__init__()
        if kwargs is None:
            kwargs = {}
        self._kwargs = kwargs
        self._args = args
        self._target = target
        self._result: Any = None

    def run(self) -> None:
        try:
            if self._target:
                self._result = self._target(*self._args, **self._kwargs)
        finally:
            del self._target, self._args, self._kwargs

    def result(self) -> Any:
        return self._result
