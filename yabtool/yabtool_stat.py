class StatMetricEntry(object):
    def __init__(self, metric_name, initial_value=None, units_name=None, additional_data=None):
        assert str(metric_name).strip()
        self._metric_name = metric_name
        self.value = initial_value
        self.units_name = units_name
        self.additional_data = additional_data

    @property
    def metric_name(self):
        return self._metric_name


class StepExecutionStatisticEntry(object):
    def __init__(self, step_name, execution_start_timestamp=None, execution_end_timestamp=None, metrics=None):
        self.step_name = step_name
        self.execution_start_timestamp = execution_start_timestamp
        self.execution_end_timestamp = execution_end_timestamp
        self.metrics = metrics if metrics else {}
