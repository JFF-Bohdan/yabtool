from .step_calculate_file_hash_and_save_to_file import StepCalculateFileHashAndSaveToFile
from .step_compress_file_with_7z import StepCompressFileWith7Z
from .step_make_directory_for_backup import StepMakeDirectoryForBackup
from .step_make_firebird_database_backup import StepMakeFirebirdDatabaseBackup, StepMakeFirebirdLinuxDatabaseBackup
from .step_make_healthchecks_ping import StepMakeHealthchecksPing
from .step_make_pg_win_database_backup import StepMakePgDatabaseWinBackup
from .step_s3_multipart_upload_with_rotation import StepS3MultipartUploadWithRotation
from .step_s3_strict_uploader import StepS3StrictUploader
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
    factory.register_class(StepMakeFirebirdLinuxDatabaseBackup)
    factory.register_class(StepCalculateFileHashAndSaveToFile)
    factory.register_class(StepCompressFileWith7Z)
    factory.register_class(StepValidate7ZArchive)
    factory.register_class(StepS3MultipartUploadWithRotation)
    factory.register_class(StepS3StrictUploader)
    factory.register_class(StepMakePgDatabaseWinBackup)
    factory.register_class(StepMakeHealthchecksPing)

    return factory
