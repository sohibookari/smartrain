import hashlib
import os
import threading
import uuid
from typing import Any, Callable

import pandas as pd

from smartrain import context as ctx
from smartrain.wr.reader import FileReader, JSONReader
from smartrain.wr.writer import FileWriter, JSONWriter


class SqlCache:
    ID_KEY = 'identity'
    PA_KEY = 'name'

    def __init__(self, path: str = None, file: str = None):
        config = ctx.get('config')
        self._logger = config.getLogger()
        self._rw_lock = threading.Lock()  # r/w lock
        self._lock = threading.Lock()  # detector lock
        self._path: str = path if path else config.CACHE_DIR
        self._file: str = file if file else config.CACHE_FILE

    def _abs(self) -> str:
        return self._path + self._file

    def exists(self) -> bool:
        return os.path.exists(self._abs())

    def _write(self, data: list) -> None:
        jw = JSONWriter(file=self._file, path=self._path)
        self._rw_lock.acquire()
        try:
            jw.write(data)
        finally:
            self._rw_lock.release()

    def _read(self) -> list:
        jr = JSONReader(file=self._file, path=self._path)
        self._rw_lock.acquire()
        try:
            return jr.read()
        finally:
            self._rw_lock.release()

    @staticmethod
    def _gen_identity(*s) -> str:
        sha1gen = hashlib.sha1()
        sha1gen.update(''.join(s).encode())
        return sha1gen.hexdigest()

    @staticmethod
    def _gen_id() -> str:
        return str(uuid.uuid4())

    def update(self, absolute_name: str, status: int, cache_file):
        in_key = self._gen_identity(absolute_name, str(status))
        in_writer = FileWriter(file=cache_file, path=self._path)
        in_data = {
            self.ID_KEY: in_key,
            self.PA_KEY: in_writer.name()
        }

        if not self.exists():
            self._write([in_data])
            return

        old_data = self._read()
        for i in old_data:
            if i[self.ID_KEY] == in_key:
                return
        old_data.append(in_data)

        self._write(old_data)

    def detect(self, absolute_name: str, status: int) -> Any:
        in_key = self._gen_identity(absolute_name, str(status))

        self._lock.acquire()
        try:
            if not self.exists():
                self.update('Default', 0, self._gen_id())
                return None
            c = self._read()
        finally:
            self._lock.release()

        for i in c:
            if i[self.ID_KEY] == in_key:
                reader = FileReader(i[self.PA_KEY], self._path)
                return reader.read_pkl()

        return None

    def fetch(self, absolute_name, status, generator: Callable, alias=None) -> pd.DataFrame:
        cache = self.detect(absolute_name, status)
        if isinstance(cache, type(None)):
            self._logger.warning("Unable to fetch the cache of %s, try to create one."
                                 % (alias if alias else '{No alias}'))
            cache_name = self._gen_id()
            cache = generator(self._path, cache_name)
            self.update(absolute_name, status, cache_name)
            self._logger.info("Successfully create the cache of %s."
                              % (alias if alias else '{No alias}'))
        else:
            self._logger.info("Successfully fetch the cache of %s."
                              % (alias if alias else '{No alias}'))

        return cache
