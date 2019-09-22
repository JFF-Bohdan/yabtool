from dupfilesremover import __version__

from setuptools import find_packages, setup


try:  # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError:  # for pip <= 9.0.3
    from pip.req import parse_requirements


PACKAGE_NAME = "dupfilesremover"

README_FILE = "README.md"
install_reqs = parse_requirements("requirements.txt", session=False)

requirements = [str(ir.req) for ir in install_reqs if ir and ir.req and str(ir.req).strip()]
hyphen_package_name = PACKAGE_NAME.replace("_", "-")
LONG_DESCTIPTION = "Please read https://github.com/JFF-Bohdan/dupfilesremover/blob/master/README.md for information"


def read_file_content(file_name):
    with open(file_name) as f:
        return f.read()


if __name__ == "__main__":
    packages_to_remove = ["tests"]
    packages = find_packages()

    for item in packages_to_remove:
        if item in packages:
            packages.remove(item)

    setup(
        name=PACKAGE_NAME,
        packages=packages,
        version=__version__,
        description="Module to support VRC-T70 hardware",
        long_description=LONG_DESCTIPTION,
        long_description_content_type="text/x-rst",
        author="Bohdan Danishevsky",
        author_email="dbn@aminis.com.ua",
        url="https://github.com/JFF-Bohdan/dupfilesremover",
        keywords=["duplicates remover", "duplicate images remover", "dups remover"],
        setup_requires=["pytest-runner"],
        tests_require=["pytest"],
        install_requires=requirements,
        classifiers=[],
        license="MIT",
        zip_safe=False,
        entry_points={
            "console_scripts": [
                "dupfilesremover=dupfilesremover.command_line.dupfilesremover:main"
            ],
        }
    )
