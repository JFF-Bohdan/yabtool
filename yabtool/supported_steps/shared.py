import subprocess


class ThirdPartyCommandsExecutor(object):
    @staticmethod
    def execute(command):
        result = subprocess.run(command, stdout=subprocess.PIPE, stdin=subprocess.PIPE)

        result.stdout = result.stdout if result.stdout is not None else bytes()
        result.stderr = result.stderr if result.stderr is not None else bytes()

        return result
