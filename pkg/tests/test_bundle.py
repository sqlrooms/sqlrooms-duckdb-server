from pathlib import Path

import duckdb
import pytest

from pkg.bundle import create_bundle, load_bundle


@pytest.fixture(scope="session")
def bundle_dir(tmpdir_factory):
    return Path(tmpdir_factory.mktemp("bundle"))


def test_bundle(bundle_dir):
    con = duckdb.connect()

    queries = [
        'CREATE TABLE IF NOT EXISTS earthquakes AS SELECT * FROM "https://pub-334685c2155547fab4287d84cae47083.r2.dev/earthquakes.parquet"',
        'SELECT count(*) FROM "earthquakes"',
    ]

    cache = {}

    assert len(cache) == 0

    create_bundle(con, cache, queries, directory=bundle_dir)

    assert len(cache) == 0

    load_bundle(con, cache, directory=bundle_dir)

    assert len(cache) == 1
