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


def test_fastani_small_num_pairs(tmpdir):
    """Test FastANI cache on small number of pairs."""

    db_file = tmpdir.mkdir("sub").join("tmp.db").strpath
    qid = 'GCF_000009045.1'
    rid = 'GCF_000186085.1'
    q_gf = './tests/data/GCF_000009045.1.fna.gz'
    r_gf = './tests/data/GCF_000186085.1.fna.gz'

    gid_pairs = [(qid, rid), (rid, qid)]
    genome_files = {qid: q_gf,
                    rid: r_gf}

    fastani = FastANI(db_file, 1)
    ani_af = fastani.pairs(gid_pairs, genome_files, report_progress=True, initial_cache_check=True)

    # exact values will depend on FastANI version, but should be reater than these bound
    assert ani_af[qid][rid][0] > 99.5
    assert ani_af[qid][rid][1] > 0.995
    assert ani_af[rid][qid][0] > 99.5
    assert ani_af[rid][qid][1] > 0.995

    # re-test results now that results have been cached
    ani_af = fastani.pairs(gid_pairs, genome_files, report_progress=True, initial_cache_check=True)
    assert ani_af[qid][rid][0] > 99.5
    assert ani_af[qid][rid][1] > 0.995
    assert ani_af[rid][qid][0] > 99.5
    assert ani_af[rid][qid][1] > 0.995


def test_fastani_large_num_pairs(tmpdir):
    """Test FastANI cache on large number of pairs."""

    db_file = tmpdir.mkdir("sub").join("tmp.db").strpath
    qid = 'GCF_000009045.1'
    rid = 'GCF_000186085.1'
    q_gf = './tests/data/GCF_000009045.1.fna.gz'
    r_gf = './tests/data/GCF_000186085.1.fna.gz'

    # artificial duplicate pairs to force using multiprocessing
    gid_pairs = [(qid, rid),  (rid, qid),
                 (qid, rid), (rid, qid),
                 (qid, rid), (rid, qid),
                 (qid, rid), (rid, qid)]
    genome_files = {qid: q_gf,
                    rid: r_gf}

    fastani = FastANI(db_file, 1)
    fastani.DB_BATCH_SIZE = 1  # force saving to cache to ensure code is tested
    ani_af = fastani.pairs(gid_pairs, genome_files, report_progress=True, initial_cache_check=True)

    # exact values will depend on FastANI version, but should be reater than these bound
    assert ani_af[qid][rid][0] > 99.5
    assert ani_af[qid][rid][1] > 0.995
    assert ani_af[rid][qid][0] > 99.5
    assert ani_af[rid][qid][1] > 0.995

    # re-test results now that results have been cached
    ani_af = fastani.pairs(gid_pairs, genome_files, report_progress=True, initial_cache_check=True)
    assert ani_af[qid][rid][0] > 99.5
    assert ani_af[qid][rid][1] > 0.995
    assert ani_af[rid][qid][0] > 99.5
    assert ani_af[rid][qid][1] > 0.995


def test_symmetric_ani():
    """Test symmetrical (max) methods for combining reciprocal pairs."""

    ani_af = {}
    ani_af['g1'] = {}
    ani_af['g1']['g2'] = (90, 0.95)
    ani_af['g2'] = {}
    ani_af['g2']['g1'] = (95, 0.90)

    fastani = FastANI(None, 1)
    ani, af = fastani.symmetric_ani(ani_af, 'g1', 'g2')
    assert ani == 95
    assert af == 0.95


def test_mean_ani():
    """Test mean methods for combining reciprocal pairs."""

    ani_af = {}
    ani_af['g1'] = {}
    ani_af['g1']['g2'] = (90, 0.95)
    ani_af['g2'] = {}
    ani_af['g2']['g1'] = (95, 0.90)

    fastani = FastANI(None, 1)
    ani, af = fastani.mean_ani(ani_af, 'g1', 'g2')
    assert ani == 92.5
    assert af == 0.925


def test_write_ani_af(tmpdir):
    """Test writing of results."""

    out_file = tmpdir.mkdir("output").join("results.tsv").strpath

    ani_af = {}
    ani_af['g1'] = {}
    ani_af['g1']['g2'] = (90, 0.95)

    fastani = FastANI(None, 1)
    fastani.write_ani_af(out_file, ani_af)

    fin = open(out_file)
    assert fin.readline().strip() == 'Query\tReference\tANI\tAF'
    assert fin.readline().strip() == 'g1\tg2\t90\t0.95'


def test_write_matrices(tmpdir):
    """Test ANI and AF matrices."""

    out_file = tmpdir.mkdir("output").join("results.tsv").strpath

    ani_af = {}
    ani_af['g1'] = {}
    ani_af['g1']['g1'] = (100, 1.0)
    ani_af['g1']['g2'] = (90, 0.95)
    ani_af['g2'] = {}
    ani_af['g2']['g2'] = (100, 1.0)
    ani_af['g2']['g1'] = (95, 0.90)

    fastani = FastANI(None, 1)
    fastani.write_ani_matrix(out_file, ani_af)

    # test ANI matrix
    fin = open(out_file)
    assert fin.readline().rstrip() == '\tg1\tg2'
    assert fin.readline().rstrip() == 'g1\t100\t90'
    assert fin.readline().rstrip() == 'g2\t95\t100'
    fin.close()

    # test AF matrix
    fastani.write_af_matrix(out_file, ani_af)
    fin = open(out_file)
    assert fin.readline().rstrip() == '\tg1\tg2'
    assert fin.readline().rstrip() == 'g1\t1.0\t0.95'
    assert fin.readline().rstrip() == 'g2\t0.9\t1.0'
    fin.close()
