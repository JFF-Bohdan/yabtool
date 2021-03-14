import codecs
import datetime
import hashlib
import os

from .base import BaseFlowStep, DryRunExecutionError


class StepCalculateFileHashAndSaveToFile(BaseFlowStep):
    def run(self, stat_entry, dry_run=False):
        input_file_name = self._render_parameter("input_file_name")
        self.step_context["input_file_name"] = input_file_name

        output_file_name = self._render_parameter("output_file_name")
        self.step_context["output_file_name"] = output_file_name

        hash_type = self.step_context["hash_type"]
        algorithms_available = [str(item).lower() for item in hashlib.algorithms_available]
        self.logger.debug(f"algorithms_available: {algorithms_available}")

        if hash_type not in algorithms_available:
            raise DryRunExecutionError(f"unsupported hash type '{hash_type}'")

        if not dry_run:
            self.logger.info(f"calculating hash ('{hash_type}') for '{input_file_name}'")

            hashing_begin_timestamp = datetime.datetime.utcnow()
            hash_value = self._hash_file(input_file_name, hash_type)
            hashing_end_timestamp = datetime.datetime.utcnow()

            metric = self._get_metric_by_name(stat_entry, "Hashed File")
            metric.value = os.path.basename(input_file_name)

            metric = self._get_metric_by_name(stat_entry, "Hash Type")
            metric.value = os.path.basename(hash_type)

            metric = self._get_metric_by_name(stat_entry, "File Size", units_name="MiB")
            size_in_mibs = self._get_file_size_in_mibs(input_file_name)
            metric.value = f"{size_in_mibs:.2f}"

            metric = self._get_metric_by_name(stat_entry, "Hash Speed", units_name="MiB/s")
            spent_time = (hashing_end_timestamp - hashing_begin_timestamp).total_seconds()

            megs_per_second = (size_in_mibs / spent_time) if spent_time else "N/A"

            if megs_per_second != "N/A":
                metric.value = f"{megs_per_second:.2f}"

            output_data = f"{hash_value} *{os.path.basename(input_file_name)}\n"
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
