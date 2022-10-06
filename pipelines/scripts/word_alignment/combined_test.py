import os

import combined


def test_make_output_dir():
    s, t, path = combined.make_output_dir(
        "fixtures/de-LU1912.txt", "fixtures/en-KJV.txt", "fixtures"
    )
    assert os.path.exists(path)
    assert s == "de-LU1912"
    assert t == "en-KJV"
    os.rmdir(path)


def test_run_fa():
    combined.run_fa("fixtures/src.txt", "fixtures/trg.txt", 0.5, "fixtures", False)
    assert os.path.exists("fixtures/src_trg_align/sorted.csv")
    assert os.path.exists("fixtures/src_trg_align/in_context.csv")
    assert os.path.exists("fixtures/src_trg_align/vref_scores.csv")
    os.remove("fixtures/src_trg_align/sorted.csv")
    os.remove("fixtures/src_trg_align/in_context.csv")
    os.remove("fixtures/src_trg_align/vref_scores.csv")
    os.rmdir("fixtures/src_trg_align")
