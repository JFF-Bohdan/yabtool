class StatMetricEntry(object):
    def __init__(self, metric_name, initial_value=None, units_name=None):
        assert str(metric_name).strip()
        self._metric_name = metric_name
        self._value = initial_value
        self.units_name = units_name

    @property
    def metric_name(self):
        return self._metric_name

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, new_value):
        self._value = new_value

    def increment(self, delta):
        self._value += delta


class MetricsHolder(object):
    def __init__(self):
        self._metrics = {}

    def get_metric(self, metric_name, initial_value=None, units_name=None):
        res = self._metrics.get(metric_name)

        if res is None:
            res = StatMetricEntry(metric_name=metric_name, initial_value=initial_value, units_name=units_name)
            self._metrics[metric_name] = res

        return res

    def get_all_metrics(self):
        return list(self._metrics.keys())

    def is_empty(self):
        return True if not self._metrics else False


class StepExecutionStatisticEntry(object):
    def __init__(
        self,
        step_name,
        step_human_readable_name=None,
        execution_start_timestamp=None,
        execution_end_timestamp=None,
        metrics=None
    ):
        self.step_name = step_name
        self.step_human_readable_name = step_human_readable_name
        self.execution_start_timestamp = execution_start_timestamp
        self.execution_end_timestamp = execution_end_timestamp
        self.metrics = MetricsHolder() if metrics is None else metrics
