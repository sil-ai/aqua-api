import pandas as pd
import pytest

__version__ = 'v2'

from prepare_semsim_data import process_idx, prepare_data

@pytest.fixture(scope='session')
def combined_bible():
    return pd.read_pickle('./fixtures/bible_combined.pkl')

@pytest.fixture(scope="session")
def condensed_df(combined_bible):
    return prepare_data(combined_bible)

def test_prepare_data_columns(condensed_df):
    expected_columns = ['src', 'tar']
    assert condensed_df.columns.to_list() == expected_columns

def test_prepare_data_shape(condensed_df, combined_bible):
    assert condensed_df.shape[0] != combined_bible.shape[0]
    assert condensed_df.shape[1] == combined_bible.shape[1]

def test_prepare_data_no_range(condensed_df):
    assert not condensed_df['src'].str.contains('<range>').any()

def test_prepare_data_combined_ref(condensed_df):
    assert ':' in condensed_df.index[-1]

def test_verse_differences(condensed_df):
    #2 Chron ranges have slight differences in the verses in the test df
    assert condensed_df.loc['John 1:3','src'] == condensed_df.loc['John 1:3','tar']
    condensed_df.loc['2 Chron 14:2-5','src'] != condensed_df.loc['2 Chron 14:2-5', 'tar']
    assert condensed_df.loc['John 1:1-2','src'] == condensed_df.loc['John 1:1-2','tar']
    condensed_df.loc['2 Chron 14:11-12','src'] != condensed_df.loc['2 Chron 14:11-12', 'tar']
    
def test_refs(condensed_df):
    assert any('-' in item for item in condensed_df.index)

def get_condensed(condensed_df):
    return condensed_df

def get_chapter_verses(chapter_df):
    chapter_verses = []
    for item in chapter_df.index:
        __, verse = item.split(':')
        try:
            int(verse)
            chapter_verses.append(int(verse))
        except ValueError:
            lower,upper = [int(item) for item in verse.split('-')]
            chapter_verses.extend(list(range(lower,upper+1)))
    return chapter_verses

@pytest.mark.parametrize(
    "chapter",
    ['2 Chron','Psalm','John']
)
def test_range_refs(chapter, condensed_df):
    chapter_df = condensed_df[condensed_df.index.str.contains(chapter)]
    chapter_verses = get_chapter_verses(chapter_df)
    assert all((chapter_verses[idx] - chapter_verses[idx-1])==1 for idx in range(1,len(chapter_verses)))
