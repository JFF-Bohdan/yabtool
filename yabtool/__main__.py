import datetime

from loguru import logger

from .app import YabtoolApplication

if __name__ == "__main__":
    tm_begin = datetime.datetime.utcnow()

    app = YabtoolApplication(logger)
    app.run()

    tm_end = datetime.datetime.utcnow()
    logger.info("app finished @ {}".format(tm_end - tm_begin))
