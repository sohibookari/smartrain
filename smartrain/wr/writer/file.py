import json

import pandas as pd
import yaml

from .base import Writer


class FileWriter(Writer):

    def __init__(self, file, path):
        super().__init__(file, path)

    def csv(self, data: pd.DataFrame):
        data.to_csv(self.abs_(), index=True, index_label="")
        self.logger.info(self.file + " has been successfully written to " + self.path)

    def pkl(self, data: pd.DataFrame):
        data.to_pickle(self.abs_())
        self.logger.info(self.file + " has been successfully written to " + self.path)


class JSONWriter(Writer):

    def __init__(self, file, path):
        super().__init__(file, path)

    def write(self, data) -> None:
        j = json.dumps(data, indent=4, sort_keys=True)
        with open(self.abs_(), mode='w') as f:
            f.write(j)
        self.logger.info(self.file + " has been successfully written to " + self.path)


class YAMLWriter(Writer):
    def __init__(self, file, path):
        super().__init__(file, path)

    def write(self, data) -> None:
        y = yaml.dump(data)
        with open(self.abs_(), mode='w') as f:
            f.write(y)
        self.logger.info(self.file + " has been successfully written to " + self.path)


class PltWriter(Writer):
    def __init__(self, file, path=None):
        super().__init__(file, path)

    def write(self, plt) -> None:
        plt.savefig(self.abs_())
        plt.close('all')
