from logging import Logger

import numpy as np
import pandas as pd

from smartrain.builder.processor import TimeProcessor, Processor
from smartrain.builder.remover import Remover
from smartrain.wr.writer.file import PltWriter
import smartrain.context as ctx


class Builder:
    def __init__(self, data):
        if not isinstance(data, pd.DataFrame):
            raise TypeError('Input data seems not a valid Dataframe object.')
        self.resource: pd.DataFrame = data
        self.result: pd.DataFrame = pd.DataFrame()
        self.logger: Logger = ctx.get('logger')

    def handle(self):
        return self

    def get_result(self) -> pd.DataFrame:
        return self.result

    def get_resource(self) -> pd.DataFrame:
        return self.resource


class TypicalTaskType:
    SUMMATION_TASK = 0
    COUNTING_TASK = 1


class FrequencySampleBuilder(Builder):

    def __init__(self, data):
        super().__init__(data)

    def handle(self):
        df = self.get_resource()
        processor = Processor(df)
        processor.count_by_group('id', 'classroom_id')
        res = processor.normalize_by_zscore('id').get_data()
        res.rename(columns={'id': 'value'}, inplace=True)

        self.result = res
        return self


class TypicalSampleBuilder(Builder):

    def __init__(self, data, task: TypicalTaskType = None):
        super().__init__(data)
        self._task = task

    def handle(self):
        df = self.get_resource()
        classroom_id_list = df['classroom_id'].drop_duplicates()
        for classroom_id in classroom_id_list:
            tdf = df[df['classroom_id'] == classroom_id].copy()
            processor = Processor(tdf)
            res = None

            if self._task == TypicalTaskType.SUMMATION_TASK:
                processor.sum_by_group('score', 'classroom_id', 'user_id')
                res = processor.normalize('score').get_data()
                res.rename(columns={'score': 'value'}, inplace=True)
            elif self._task == TypicalTaskType.COUNTING_TASK:
                processor.count_by_group('id', 'classroom_id', 'user_id')
                res = processor.normalize('id').get_data()
                res.rename(columns={'id': 'value'}, inplace=True)

            if len(self.result) == 0:
                self.result = res
            else:
                self.result = pd.concat([self.result, res], ignore_index=True)

        return self


class CheckInSampleBuilder(Builder):
    def __init__(self, data):
        super().__init__(data)

    def handle(self, visualize=False):
        df = self.get_resource()
        lesson_id_list = df['lesson_id'].drop_duplicates()
        for lesson_id in lesson_id_list:
            tdf = df[df['lesson_id'] == lesson_id].copy()
            if len(tdf) < 20:
                df = Remover(df).select_by_colvalue('lesson_id', lesson_id).remove()
                continue

            processor = TimeProcessor(tdf['timestamp'])
            pred = processor.scale_by_zscore().evaluate_by_EllipticEnvelope()
            scaled_data = processor.get_data()
            pred[np.argwhere(scaled_data <= 0)] = 1

            if visualize:
                import seaborn as sns
                from matplotlib import pyplot as plt
                tdf['timestamp'] = scaled_data
                tdf['status'] = pred
                plt.figure()
                sns.relplot(data=tdf, x=[i for i in tdf.index], y="timestamp", hue="status", style="status")
                PltWriter("%d.png" % lesson_id).write(plt)

            df.loc[tdf.index, 'score'] = pred
        self.result = df

        return self


class SampleSetMerger(Builder):
    def __init__(self, data_set, key_cols):
        super().__init__(pd.DataFrame())
        self.resource = data_set
        self.key_cols = key_cols

    def handle(self):
        df = None
        for item in self.resource:
            name = item[0]
            tdf = item[1]
            tdf.rename(columns={'value': name}, inplace=True)
            self.logger.debug('table %s is on merging process.' % name)
            if not isinstance(df, pd.DataFrame):
                df = tdf
            else:
                df = pd.merge(df, tdf, how='outer', on=self.key_cols)
        self.result = df

        return self
