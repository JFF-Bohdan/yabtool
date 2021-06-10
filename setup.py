from setuptools import find_packages, setup  # noqa
from yabtool import __version__  # noqa

try:  # for pip >= 10
    from pip._internal.req import parse_requirements  # noqa
except ImportError:  # for pip <= 9.0.3
    from pip.req import parse_requirements  # noqa


PACKAGE_NAME = "yabtool"

README_FILE = "README.md"
install_reqs = parse_requirements("requirements.txt", session=False)

requirements = [str(ir.requirement) for ir in install_reqs
                if ir and ir.requirement and str(ir.requirement).strip()]
hyphen_package_name = PACKAGE_NAME.replace("_", "-")
LONG_DESCTIPTION = "Please read https://github.com/JFF-Bohdan/yabtool/blob/master/README.md for information"


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
        package_dir={
            PACKAGE_NAME: PACKAGE_NAME
        },
        package_data={
            PACKAGE_NAME: ["./config/*.yaml"]
        },
        version=__version__,
        description="Module to perform scheduled backups for FB/PG databases with support for backups uploading "
                    "into S3 (with rotation) and support of integration with Healchecks.io",
        long_description=LONG_DESCTIPTION,
        long_description_content_type="text/x-rst",
        author="Bohdan Danishevsky",
        author_email="dbn@aminis.com.ua",
        url="https://github.com/JFF-Bohdan/yabtool",
        keywords=["flexible backup tool", "configurable multipurpose backup tool", "backup tool"],
        setup_requires=["pytest-runner"],
        tests_require=["pytest"],
        install_requires=requirements,
        classifiers=[],
        license="MIT",
        zip_safe=False
    )
