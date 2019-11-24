import os

from .base import BaseFlowStep


class StepMakeDirectoryForBackup(BaseFlowStep):
    def run(self, stat_entry, dry_run=False):

        res = self._render_parameter("generation_mask")
        res = os.path.normpath(os.path.abspath(res))

        if not dry_run:
            os.makedirs(res)

        self.additional_output_context = {"result": res}

        return super().run(dry_run)

    @classmethod
    def step_name(cls):
        return "mkdir_for_backup"
