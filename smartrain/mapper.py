from smartrain.builder import TypicalSampleBuilder, TypicalTaskType, CheckInSampleBuilder, Processor


class Mapper:

    def _do_SUMMATION_TASK(self, data):
        builder = TypicalSampleBuilder(data, task=TypicalTaskType.SUMMATION_TASK)
        res = builder.handle().get_result()
        return res

    def _do_COUNTING_TASK(self, data):
        builder = TypicalSampleBuilder(data, task=TypicalTaskType.COUNTING_TASK)
        res = builder.handle().get_result()
        return res

    def _do_CHECKIN_TASK(self, data):
        builder = CheckInSampleBuilder(data)
        res = builder.handle().get_result()
        res.replace(to_replace=-1, value=0, inplace=True)
        return self._do_SUMMATION_TASK(res)

    def _do_PREP_TASK(self, data):
        res = Processor(data).get_ratio('score', 'depth', 'count').get_data()
        return self._do_SUMMATION_TASK(res)
