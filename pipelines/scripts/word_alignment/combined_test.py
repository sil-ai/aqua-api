import os
from pathlib import Path

import combined


def test_make_output_dir():
    s, t, path = combined.make_output_dir(
        Path("fixtures/de-LU1912.txt"), Path("fixtures/en-KJV.txt"), Path("fixtures")
    )
    assert path.exists()
    assert s == "de-LU1912"
    assert t == "en-KJV"
    Path.rmdir(path)


def test_run_fa():
    combined.run_fa(
        Path("fixtures/src.txt"), Path("fixtures/trg.txt"), 0.5, Path("fixtures"), False
    )
    assert os.path.exists("fixtures/src_trg_align/sorted.csv")
    assert os.path.exists("fixtures/src_trg_align/in_context.csv")
    assert os.path.exists("fixtures/src_trg_align/vref_scores.csv")
    os.remove("fixtures/src_trg_align/sorted.csv")
    os.remove("fixtures/src_trg_align/in_context.csv")
    os.remove("fixtures/src_trg_align/vref_scores.csv")
    os.rmdir("fixtures/src_trg_align")
