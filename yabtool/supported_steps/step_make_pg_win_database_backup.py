import os

from .base import BaseFlowStep
from .shared import ThirdPartyCommandsExecutor


class StepMakePgDatabaseWinBackup(BaseFlowStep):
    def run(self, stat_entry, dry_run=False):
        os.environ["PGPASSWORD"] = self._render_parameter("db_password")

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

        del os.environ["PGPASSWORD"]

        if not dry_run:
            backup_log_name = self._render_parameter("backup_log_name")
            self.logger.debug(f"Saving log file from PG backup tool into {backup_log_name}")
            self._save_backup_log(backup_log_name, result.stderr)

            result.check_returncode()

        return super().run(dry_run)

    @staticmethod
    def _save_backup_log(backup_log_name: str, content: bytes):
        with open(backup_log_name, "wb") as output_file:
            output_file.write(content)

    @classmethod
    def step_name(cls):
        return "pg_win_backup"
