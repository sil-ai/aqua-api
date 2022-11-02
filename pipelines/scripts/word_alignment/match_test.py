import pandas as pd
import match
import json
import pytest
from pathlib import Path


def test_text_to_words():
    text = "This is a test."
    words = match.text_to_words(text)
    assert words == ["this", "is", "a", "test", ""]

@pytest.mark.parametrize("word,expected", [
                                            (None, ''),
                                            ('מַלְכִּיאֵֽל', 'מלכיאל')
                                           ])
def test_normalize_word(word, expected):
    assert match.normalize_word(word) == expected


def test_get_bible_data():
    df = match.get_text_data("fixtures/en-KJV.txt")
    assert len(df) > 5000
    assert isinstance(df, pd.DataFrame)
    assert 'god' in df['words'].explode().unique()

@pytest.mark.parametrize("index_cache_keys,index_cache_values", [
                                                    (Path("fixtures/en-NIV84-index-cache.json"), Path("fixtures/hbo-MaculaHebTok-index-cache.json")), 

                                                    ])
def test_update_matches_for_lists(index_cache_keys, index_cache_values):
    source_list = [
        "",
        "ishvi",
        "their",
        "was",
        "sons",
        "ishvah",
        "beriah",
        "heber",
        "the",
        "sister",
        "and",
        "malkiel",
        "imnah",
        "asher",
        "serah",
        "of",
    ]
    target_list = [
        "בְנֵ֣י",
        "בְנֵ֣י",
        "אָשֵׁ֗ר",
        "יִמְנָ֧ה",
        "יִשְׁוָ֛ה",
        "יִשְׁוָ֛ה",
        "יִשְׁוִ֥י",
        "וּ",
        "בְרִיעָ֖ה",
        "בְרִיעָ֖ה",
        "בְרִיעָ֖ה",
        "ם",
        "בְנֵ֣י",
        "בְנֵ֣י",
        "בְנֵ֣י",
        "בְרִיעָ֔ה",
        "חֶ֖בֶר",
        "וּ",
        "מַלְכִּיאֵֽל",
    ]
    source_objects = {word: match.Word(word) for word in source_list}
    target_objects = {word: match.Word(word) for word in target_list}
    for word in source_objects.values():
        word.index_list = [0]
    for word in target_objects.values():
        word.index_list = [0]
    js_cache = {}
    matches = {}
    jaccard_similarity_threshold = 0.0
    count_threshold = 0

    matches, js_cache = match.update_matches_for_lists(
        source_objects.values(),
        target_objects.values(),
        js_cache,
        matches,
        jaccard_similarity_threshold,
        count_threshold,
    )
    assert len(matches) > 0
    assert len(matches["the"]) > 0
    assert len(js_cache) > 0
    assert isinstance(matches, dict)
    assert isinstance(js_cache, dict)


@pytest.mark.parametrize("source,target,source_word,target_word", [
                                                    (Path("fixtures/es-test.txt"), Path("fixtures/en-test.txt"), 'televisión', 'reservation'), 
                                                    (Path("fixtures/de-LU1912-mini.txt"), Path("fixtures/en-KJV-mini.txt"), 'licht', 'good'), 

                                                    ])
def test_run_match(source, target, source_word, target_word, remove_files = False):
    outpath = source.parent / 'out' / f'{source.stem}_{target.stem}'
    match.run_match(
        source,
        target,
        outpath = outpath,
        logging_level='INFO',
        jaccard_similarity_threshold=0.0,
        count_threshold=0,
        refresh_cache=True,
    )
    
    with open(outpath / 'dictionary.json') as f:
        dictionary = json.load(f)
    assert(len(dictionary) > 0)
    assert source_word in dictionary
    
    with open(outpath / 'cache' / f'{source.stem}-{target.stem}-freq-cache.json') as f:
        freq_cache = json.load(f)
    assert f'{source_word}-{target_word}' in freq_cache
    
    with open(outpath / 'cache' / f'{source.stem}-index-cache.json') as f:
        keys_index_cache = json.load(f)
    assert source_word in keys_index_cache
    assert isinstance(keys_index_cache[source_word], list)
    assert len(keys_index_cache[source_word]) > 0

    with open(outpath / 'cache' / f'{target.stem}-index-cache.json') as f:
        values_index_cache = json.load(f)
    assert target_word in values_index_cache
    assert isinstance(values_index_cache[target_word], list)
    assert len(values_index_cache[target_word]) > 0

    (outpath / 'dictionary.json').unlink()
    (outpath / 'ref_df.csv').unlink()