import pandas as pd
import match
import json

def test_text_to_words():
    text = "This is a test."
    words = match.text_to_words(text)
    assert words == ["this", "is", "a", "test"]


def test_get_bible_data():
    df = match.get_bible_data("fixtures/en-KJV.txt")
    assert len(df) > 0
    assert type(df) == pd.DataFrame

def test_update_matches_for_lists():
    keys_list = ['',
                'ishvi',
                'their',
                'was',
                'sons',
                'ishvah',
                'beriah',
                'heber',
                'the',
                'sister',
                'and',
                'malkiel',
                'imnah',
                'asher',
                'serah',
                'of']
    values_list = ['בְנֵ֣י',
                'בְנֵ֣י',
                'אָשֵׁ֗ר',
                'יִמְנָ֧ה',
                'יִשְׁוָ֛ה',
                'יִשְׁוָ֛ה',
                'יִשְׁוִ֥י',
                'וּ',
                'בְרִיעָ֖ה',
                'בְרִיעָ֖ה',
                'בְרִיעָ֖ה',
                'ם',
                'בְנֵ֣י',
                'בְנֵ֣י',
                'בְנֵ֣י',
                'בְרִיעָ֔ה',
                'חֶ֖בֶר',
                'וּ',
                'מַלְכִּיאֵֽל',
                ]
    js_cache = {}
    matches = {}
    with open ('fixtures/en-NIV84-index-cache.json') as f:
        keys_index = json.load(f)
    with open ('fixtures/hbo-MaculaHebTok-index-cache.json') as f:
        values_index = json.load(f)
    jaccard_similarity_threshold = 0.0
    count_threshold = 0
    
    matches, js_cache = match.update_matches_for_lists(keys_list, values_list, js_cache, matches, keys_index, values_index, jaccard_similarity_threshold, count_threshold)
    assert len(matches) > 0
    assert len(matches['the']) > 0
    assert len(js_cache) > 0
    assert type(matches) == dict
    assert type(js_cache) == dict
