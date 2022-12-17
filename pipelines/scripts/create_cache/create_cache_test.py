from pathlib import Path
import json
import argparse
import pytest

import pandas as pd

import create_cache

@pytest.mark.parametrize("source,is_bible", [
                                                    (Path("fixtures/in/es-NTV-mini.txt"), True),
                                                    (Path("fixtures/in/de-LU1912-mini.txt"), True),
                                                    (Path("fixtures/in/en-KJV-mini.txt"), True),
                                                    ])
def test_create_index_cache(source, is_bible):
    outpath = Path(f'fixtures/out/{source.stem}')
    outpath.mkdir(exist_ok=True, parents=True)

    index_cache_file = create_cache.create_index_cache(source, outpath, is_bible=is_bible)

    with open(index_cache_file) as f:
        index_cache = json.load(f)
    
    assert len(index_cache) > 10
    print(index_cache)    


@pytest.mark.parametrize("source,is_bible", [
                                                    (Path("fixtures/in/es-NTV-mini.txt"), True),
                                                    (Path("fixtures/in/de-LU1912-mini.txt"), True),
                                                    (Path("fixtures/in/en-KJV-mini.txt"), True),
                                                    ])
def test_create_meta_file(source, is_bible):
    outpath = Path(f'fixtures/out/{source.stem}')
    outpath.mkdir(exist_ok=True, parents=True)

    meta_file = create_cache.create_meta_file(source, outpath)

    with open(meta_file) as f:
        meta = json.load(f)

    assert 'source' in meta
    assert isinstance(meta['source'], str)
    assert meta['source'] == source.stem