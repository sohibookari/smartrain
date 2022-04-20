from logging import Logger

import numpy as np
import smartrain.context as ctx
from smartrain.builder import (TypicalSampleBuilder, TypicalTaskType, CheckInSampleBuilder, SampleSetMerger,
                               FrequencySampleBuilder, Processor, Remover)
from smartrain.wr import FileWriter


class Mapper:
    def __init__(self):
        self.logger: Logger = ctx.get('logger')

    def _do_SUMMATION_TASK(self, data, name=None):
        builder = TypicalSampleBuilder(data, task=TypicalTaskType.SUMMATION_TASK)
        res = builder.handle().get_result()
        return res

    def _do_COUNTING_TASK(self, data, name=None):
        builder = TypicalSampleBuilder(data, task=TypicalTaskType.COUNTING_TASK)
        res = builder.handle().get_result()
        return res

    def _do_CHECKIN_TASK(self, data, name=None):
        builder = CheckInSampleBuilder(data)
        res = builder.handle().get_result()
        res.replace(to_replace=-1, value=0, inplace=True)
        return self._do_SUMMATION_TASK(res)

    def _do_PREP_TASK(self, data, name=None):
        res = Processor(data).get_ratio('score', 'depth', 'count').get_data()
        return self._do_SUMMATION_TASK(res)

    def _do_MERGE_TASK(self, data, name=None):
        builder = SampleSetMerger(data, key_cols=['classroom_id', 'user_id'])
        res = builder.handle().get_result()
        res = Remover(res).select_by_null('exam').remove()
        self.logger.debug(res.info())
        return res

    def _do_FREQUENCY_TASK(self, data, name=None):
        builder = FrequencySampleBuilder(data)
        res = builder.handle().get_result()
        return res

    def _do_FREQUENCY_MERGE_TASK(self, data, name=None):
        builder = SampleSetMerger(data, key_cols=['classroom_id'])
        res = builder.handle().get_result()
        res = Remover(res).select_by_null('exam').remove()
        self.logger.debug(res.info())
        return res

    def _do_LABEL_TASK(self, data, name=None):
        res = Processor(data).cluster('value', 3).get_data()
        res.rename(columns={'cluster': '%s_cluster' % name}, inplace=True)
        return res

    def _do_OUTPUT_TASK(self, data, name):
        FileWriter('%s.csv' % name).csv(data)
