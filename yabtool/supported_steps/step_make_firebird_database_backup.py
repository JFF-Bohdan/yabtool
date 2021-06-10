from .base import BaseFlowStep
from .shared import ThirdPartyCommandsExecutor


class StepMakeFirebirdDatabaseBackup(BaseFlowStep):
    def run(self, stat_entry, dry_run=False):
        backup_log_name = self._render_parameter("backup_log_name")
        self.step_context["backup_log_name"] = backup_log_name

        backup_file_name = self._render_parameter("backup_file_name")
        self.step_context["backup_file_name"] = backup_file_name

        command = self._render_parameter("command_template")
        self.step_context["command"] = command

        dry_run_command = self._render_parameter("dry_run_command")
        self.step_context["dry_run_command"] = dry_run_command

        if not dry_run:
            self.logger.info("Making backup of Firebird database")
            self.logger.debug("going to execute: {}".format(command))
            result = ThirdPartyCommandsExecutor.execute(command)
        else:
            self.logger.debug("going to execute: {}".format(dry_run_command))
            result = ThirdPartyCommandsExecutor.execute(dry_run_command)

        if not dry_run:
            result.check_returncode()

        return super().run(dry_run)

    @classmethod
    def step_name(cls):
        return "firebird_backup"


class StepMakeFirebirdLinuxDatabaseBackup(StepMakeFirebirdDatabaseBackup):
    @classmethod
    def step_name(cls):
        return "linux_firebird_backup"
