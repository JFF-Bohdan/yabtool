from .s3_file_upload import S3FileUpload
from .supported_steps import (CalculateFileHashAndSaveToFile, CompressFileWithSevenZ, MakeDirectoryForBackup,
                              MakeFirebirdDatabaseBackup, Validate7ZArchive)


class StepsFactory(object):
    def __init__(self):
        self._known_steps = dict()
        pass

    def register_class(self, cls):
        self._known_steps[cls.step_name()] = cls

    def create_object(self, step_name, **kwargs):
        return self._known_steps[step_name](**kwargs)

    def is_step_known(self, step_name):
        return step_name in self._known_steps


def create_steps_factory():
    factory = StepsFactory()

    factory.register_class(MakeDirectoryForBackup)
    factory.register_class(MakeFirebirdDatabaseBackup)
    factory.register_class(CalculateFileHashAndSaveToFile)
    factory.register_class(CompressFileWithSevenZ)
    factory.register_class(Validate7ZArchive)
    factory.register_class(S3FileUpload)

    return factory
