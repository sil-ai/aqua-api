import pandas as pd
import json
from pathlib import Path

import pytest

import get_data, match_contexts

@pytest.mark.parametrize("word,expected", [
                                            ('test', 'test'),
                                            ('מַלְכִּיאֵֽל', 'מלכיאל')
                                           ])
def test_normalize_word(word, expected):
    assert get_data.normalize_word(word) == expected


@pytest.mark.parametrize("source,target,is_bible", [
                                            (Path("fixtures/in/de-LU1912-mini/de-LU1912-mini.txt"), Path("fixtures/in/en-KJV-mini/en-KJV-mini.txt"), True), 
                                            (Path("fixtures/in/en-KJV-mini/en-KJV-mini.txt"), Path("fixtures/in/es-NTV-mini/es-NTV-mini.txt"), True),
                                            (Path("fixtures/in/es-NTV-mini/es-NTV-mini.txt"), Path("fixtures/in/de-LU1912-mini/de-LU1912-mini.txt"), True) 
                                            ])
def test_run_match(source, target, is_bible):
    outpath = Path('fixtures/out') / f'{source.stem}_{target.stem}'
    outpath.mkdir(parents=True, exist_ok=True)
    match_contexts.run_match(
        source,
        target,
        outpath = outpath,
        jaccard_similarity_threshold=0.0,
        count_threshold=0,
        refresh_cache=True,
        is_bible=is_bible,
    )
    
    with open(outpath / 'dictionary.json') as f:
        dictionary = json.load(f)
    assert(len(dictionary) > 0)
    