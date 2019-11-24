import os
import sys

dir_name = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(dir_name, ".."))

from yabtool import __version__  # noqa


def test_version_string_is_not_empty():
    assert __version__
