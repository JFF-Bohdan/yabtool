import subprocess


class ThirdPartyCommandsExecutor(object):
    @staticmethod
    def execute(command, shell: bool = True):
        result = subprocess.run(command, stdout=subprocess.PIPE, stdin=subprocess.PIPE, shell=shell)

        result.stdout = result.stdout if result.stdout is not None else bytes()
        result.stderr = result.stderr if result.stderr is not None else bytes()

        return result
