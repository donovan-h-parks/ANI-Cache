# PyTest unit testing for utils.py

import pytest

from ani_cache.utils import (check_file_exists,
                             is_executable,
                             which,
                             check_on_path,
                             check_dependencies)


def test_check_file_exists():
    with pytest.raises(SystemExit):
        check_file_exists('missing_file')


def test_is_executable():
    assert not is_executable('missing_program')


def test_which():
    assert which('missing_program') is None


def test_check_on_path():
    assert not check_on_path('missing_path', exit_on_fail=False)

    with pytest.raises(SystemExit):
        check_on_path('missing_program', exit_on_fail=True)


def test_check_dependencies():
    assert not check_dependencies(['missing_program'], exit_on_fail=False)

    with pytest.raises(SystemExit):
        check_dependencies(['missing_program'], exit_on_fail=True)
