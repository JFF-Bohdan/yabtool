import datetime

from .app import YabtoolApplication


if __name__ == "__main__":
    tm_begin = datetime.datetime.utcnow()

    app = YabtoolApplication()
    app.run()
    tm_end = datetime.datetime.utcnow()

    app.logger.info("app finished @ {}".format(tm_end - tm_begin))
    tm_end = datetime.datetime.utcnow()
