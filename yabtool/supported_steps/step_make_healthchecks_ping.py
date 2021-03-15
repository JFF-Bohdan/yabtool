import requests

from .base import BaseFlowStep


class StepMakeHealthchecksPing(BaseFlowStep):
    def run(self, stat_entry, dry_run=False):
        healthchecks_io_url = self.secret_context["healthchecks_io_url"]

        if not dry_run:
            if healthchecks_io_url:
                requests.post(healthchecks_io_url)
            else:
                self.logger.warning("No healthcheck endpoint added for ping")
        else:
            self.logger.info("Skipping ping to healthchecks.io because of dry run")
            return

        return super().run(dry_run)

    @staticmethod
    def _save_backup_log(backup_log_name: str, content: bytes):
        with open(backup_log_name, "wb") as output_file:
            output_file.write(content)

    @classmethod
    def step_name(cls):
        return "healthchecks_ping"
