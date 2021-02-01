# PyTest unit testing for fastani.py

import pytest

from ani_cache.exceptions import FastANIError
from ani_cache.fastani import FastANI


def test_object_db_creation(tmpdir):
    """Test creation of FastANI object with SQlite database."""

    db_file = tmpdir.mkdir("sub").join("tmp.db").strpath

    fastani = FastANI(db_file, 1)

    assert fastani.num_db_rows() is None


def test_fastani_invalid_input(tmpdir):
    """Test FastANI on invalid input."""

    db_file = tmpdir.mkdir("sub").join("tmp.db").strpath
    qid = 'invalid1'
    rid = 'invalid2'
    q_gf = 'does_not_exist.fna'
    r_gf = 'does_not_exist.fna'

    fastani = FastANI(db_file, 1)

    with pytest.raises(FastANIError):
        fastani.fastani(qid, rid, q_gf, r_gf)


def test_fastani_valid_input(tmpdir):
    """Test FastANI on valid input."""

    db_file = tmpdir.mkdir("sub").join("tmp.db").strpath
    qid = 'GCF_000009045.1'
    rid = 'GCF_000186085.1'
    q_gf = './tests/data/GCF_000009045.1.fna.gz'
    r_gf = './tests/data/GCF_000186085.1.fna.gz'

    fastani = FastANI(db_file, 1)

    ani_af = fastani.fastani(qid, rid, q_gf, r_gf)

    assert ani_af[0] == 'GCF_000009045.1'
    assert ani_af[1] == 'GCF_000186085.1'

    # exact values will depend on FastANI version, but should be reater than these bound
    assert ani_af[2] > 99.5
    assert ani_af[3] > 0.995
