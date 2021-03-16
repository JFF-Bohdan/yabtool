import datetime

from .supported_steps.base import pretty_time_delta, time_interval
from .yabtool_application import YabtoolApplication


if __name__ == "__main__":
    timestamp_start = datetime.datetime.utcnow()

    app = YabtoolApplication()
    app.run(args=None)

    timestamp_end = datetime.datetime.utcnow()
    seconds_spent = time_interval(timestamp_start, timestamp_end)

    app.logger.info("app finished @ {}".format(pretty_time_delta(seconds_spent)))
