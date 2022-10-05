import pandas as pd
import match


def test_text_to_words():
    text = "This is a test."
    words = match.text_to_words(text)
    assert words == ["this", "is", "a", "test"]


def test_get_bible_data():
    df = match.get_bible_data("fixtures/en-KJV.txt")
    assert len(df) > 0
    assert type(df) == pd.DataFrame
