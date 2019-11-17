import codecs
import hashlib
import os

from .base import BaseFlowStep, DryRunExecutionError


class StepCalculateFileHashAndSaveToFile(BaseFlowStep):
    def run(self, dry_run=False):
        input_file_name = self._render_parameter("input_file_name")
        self.step_context["input_file_name"] = input_file_name

        output_file_name = self._render_parameter("output_file_name")
        self.step_context["output_file_name"] = output_file_name

        hash_type = self.step_context["hash_type"]
        algorithms_available = [str(item).lower() for item in hashlib.algorithms_available]
        self.logger.debug("algorithms_available: {}".format(algorithms_available))

        if hash_type not in algorithms_available:
            raise DryRunExecutionError("unsupported hash type '{}'".format(hash_type))

        if not dry_run:
            self.logger.info("going calculate hash ('{}') for '{}'".format(hash_type, input_file_name))
            hash_value = self._hash_file(input_file_name, hash_type)
            output_data = "{} *{}\n".format(hash_value, os.path.basename(input_file_name))
            self._save_data(output_file_name, output_data)

        return super().run(dry_run)

    @staticmethod
    def _save_data(file_name, data, codepage="utf-8"):
        with codecs.open(file_name, "w", codepage) as output_file:
            output_file.write(data)

    @staticmethod
    def _hash_file(file_name, hash_type):
        BLOCKSIZE = 65536

        hasher = hashlib.new(hash_type)
        with open(file_name, "rb") as afile:
            buf = afile.read(BLOCKSIZE)
            while len(buf) > 0:
                hasher.update(buf)
                buf = afile.read(BLOCKSIZE)

        return str(hasher.hexdigest())

    @classmethod
    def step_name(cls):
        return "calculate_file_hash_and_save_in_file"
