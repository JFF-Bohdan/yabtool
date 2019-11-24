from .step_calculate_file_hash_and_save_to_file import StepCalculateFileHashAndSaveToFile
from .step_compress_file_with_7z import StepCompressFileWith7Z
from .step_make_directory_for_backup import StepMakeDirectoryForBackup
from .step_make_firebird_database_backup import StepMakeFirebirdDatabaseBackup
from .step_s3_file_upload import StepS3FileUpload
from .step_validate_7z_archive import StepValidate7ZArchive


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

    factory.register_class(StepMakeDirectoryForBackup)
    factory.register_class(StepMakeFirebirdDatabaseBackup)
    factory.register_class(StepCalculateFileHashAndSaveToFile)
    factory.register_class(StepCompressFileWith7Z)
    factory.register_class(StepValidate7ZArchive)
    factory.register_class(StepS3FileUpload)

    return factory
