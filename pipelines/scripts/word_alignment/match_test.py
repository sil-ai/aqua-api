import pandas as pd
import match
import json
import pytest
from pathlib import Path

import get_data

@pytest.mark.parametrize("word,expected", [
                                            (None, ''),
                                            ('מַלְכִּיאֵֽל', 'מלכיאל')
                                           ])
def test_normalize_word(word, expected):
    assert get_data.normalize_word(word) == expected


def test_get_bible_data():
    outpath = Path('')
    ref_df = get_data.get_ref_df("fixtures/en-KJV.txt")
    ref_df = get_data.get_words_from_txt_file(ref_df, outpath)
    assert len(ref_df) == 41899
    assert isinstance(ref_df, pd.DataFrame)
    assert 'god' in ref_df['src_words'].explode().unique()

@pytest.mark.parametrize("cache_dir,source,target", [
                                                    (Path("fixtures"), Path('fixtures/en-NIV84.txt'), Path("fixtures/hbo-MaculaHebTok.txt")), 

                                                    ])
def test_update_matches_for_lists(cache_dir, source, target):
    outpath = Path('')
    is_bible=True
    refresh_cache=False
    word_dict_src = get_data.create_words(source, cache_dir, outpath, is_bible=is_bible, refresh_cache=refresh_cache)
    word_dict_trg = get_data.create_words(target, cache_dir, outpath, is_bible=is_bible, refresh_cache=refresh_cache)
    ref_df = get_data.get_ref_df(source, target, is_bible=is_bible)
    condensed_df = get_data.condense_files(ref_df)
    condensed_df = get_data.get_words_from_txt_file(condensed_df, outpath)
    condensed_df.loc[:, 'normalized_src_words'] = condensed_df['src'].apply(lambda x: get_data.normalize_word(x).split())
    condensed_df.loc[:, 'normalized_trg_words'] = condensed_df['trg'].apply(lambda x: get_data.normalize_word(x).split())
    
    condensed_df_indexes = list(condensed_df.index)  
    for word_object in word_dict_src.values():
        word_object.reduced_index_list = list(set(word_object.index_list).intersection(set(condensed_df_indexes)))
    for word_object in word_dict_trg.values():
        word_object.reduced_index_list = list(set(word_object.index_list).intersection(set(condensed_df_indexes)))
    
    matches = {}
    print("Getting matches...")
    for _, row in condensed_df.iterrows():
        paired_source_objects = list(set([word_dict_src[x] for x in row["src_words"]]))
        paired_target_objects = list(set([word_dict_trg[x] for x in row["trg_words"]]))
        # for paired_object in [*paired_source_objects, *paired_target_objects]:
            # paired_object.index_list = set(paired_object.index_list).intersection(set(ref_df_indexes))
        matches, js_cache = match.update_matches_for_lists(
            paired_source_objects,
            paired_target_objects,
            matches=matches,
            js_cache=js_cache,
            jaccard_similarity_threshold=0.05,
            count_threshold=0,
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
        jaccard_similarity_threshold=0.0,
        count_threshold=0,
        refresh_cache=True,
        is_bible=False,
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
