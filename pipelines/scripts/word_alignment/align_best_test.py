from pathlib import Path
import pytest
import align_best
import pandas as pd

@pytest.mark.parametrize("source,target", [
                                            (Path("fixtures/es-test.txt"), Path("fixtures/en-test.txt")), 
                                            (Path("fixtures/es-test.txt"), Path("fixtures/en-test.txt")), 
                                            ])
def test_get_alignments(source, target):
    vrefs = align_best.get_ref_df(source, target, False)
    outpath =  source.parent / 'out' / f'{source.stem}_{target.stem}'
    condensed_source, condensed_target = align_best.write_condensed_files(source, target, outpath)
    corpus = align_best.create_corpus(condensed_source, condensed_target)
    model = align_best.train_model(corpus)
    alignments = align_best.get_best_alignment_scores(model, corpus, vrefs)

    assert type(alignments) == pd.DataFrame
    assert len(alignments) > 0
    assert len(alignments['alignment_score'].unique()) > 10
    assert len(alignments['verse_score'].unique()) > 3
    assert len(alignments['source'].unique()) > 10
    assert len(alignments['target'].unique()) > 10
    assert len(alignments['alignment_count'].unique()) == 1
    (outpath / f'{source.stem}_condensed.txt').unlink()
    (outpath / f'{target.stem}_condensed.txt').unlink()


@pytest.mark.parametrize("source,target,is_bible", [
                                                    (Path("fixtures/es-test.txt"), Path("fixtures/en-test.txt"), False), 
                                                    (Path("fixtures/de-LU1912-mini.txt"), Path("fixtures/en-KJV-mini.txt"), True),
                                                    ])
def test_get_vref_scores(source, target, is_bible):
    outpath =  source.parent / 'out' / f'{source.stem}_{target.stem}'
    condensed_source, condensed_target = align_best.write_condensed_files(source, target, outpath)
    vrefs = align_best.get_ref_df(source, target, is_bible=is_bible)
    corpus = align_best.create_corpus(condensed_source, condensed_target)
    model = align_best.train_model(corpus)
    alignments = align_best.get_best_alignment_scores(model, corpus, vrefs)
    vref_scores = align_best.get_vref_scores(alignments)
    assert type(vref_scores) == pd.DataFrame
    assert len(vref_scores) > 0
    assert len(vref_scores['verse_score'].unique()) > len(vrefs) / 2
    (outpath / f'{source.stem}_condensed.txt').unlink()
    (outpath / f'{target.stem}_condensed.txt').unlink()

@pytest.mark.parametrize("source,target", [
                                                    (Path("fixtures/es-test.txt"), Path("fixtures/en-test.txt")), 
                                                    (Path("fixtures/de-LU1912-mini.txt"), Path("fixtures/en-KJV-mini.txt")), 
                                                    ])
def test_get_best_alignment_scores(source, target):
    is_bible=False
    outpath = source.parent / 'out' / f'{source.stem}_{target.stem}'
    condensed_source, condensed_target = align_best.write_condensed_files(source, target, outpath)
    corpus = align_best.create_corpus(condensed_source, condensed_target)
    vrefs = align_best.get_ref_df(source, target, is_bible)
    model = align_best.train_model(corpus)
    df = align_best.get_best_alignment_scores(model, corpus, vrefs)
    assert len(df) > 20
    assert len(df['alignment_score'].unique()) > 20
    assert len(df['verse_score'].unique()) > 1

    df.to_csv(outpath / 'df_best_alignment_scores.csv')

    (outpath / f"{source.stem}_condensed.txt").unlink()
    (outpath / f"{target.stem}_condensed.txt").unlink()


@pytest.mark.parametrize("outpath", [
                                                    Path("fixtures/out/es-test_en-test"), 
                                                    Path("fixtures/out/de-LU1912-mini_en-KJV-mini"), 
                                                    ])
def test_remove_duplicates(outpath):
    df = pd.read_csv(outpath / 'df_best_alignment_scores.csv')
    df = align_best.remove_duplicates(df)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert min(df['alignment_count']) >= 1
    assert max(df['verse_score']) <= 1
    assert min(df['verse_score']) >= 0
    assert len(df['verse_score'].unique()) >= 5
    assert max(df['alignment_score']) <= 1
    assert min(df['alignment_score']) >= 0
    assert len(df['alignment_score'].unique()) >= 20
    (outpath / 'df_best_alignment_scores.csv').unlink()
    # df.to_csv(outpath / f'df_best_remove_duplicates.csv')


@pytest.mark.parametrize("source,target,is_bible", [
                                                    (Path("fixtures/es-test.txt"), Path("fixtures/en-test.txt"), False), 
                                                    (Path("fixtures/de-LU1912-mini.txt"), Path("fixtures/en-KJV-mini.txt"), True),
                                                    ])
def test_run_best_align(source, target, is_bible, delete_files=True):
    outpath = source.parent / 'out' / f'{source.stem}_{target.stem}'

    align_best.run_best_align(source, target, outpath, is_bible=is_bible)

    # check forward files exist
    best_in_context = Path(outpath, "best_in_context.csv")
    best_sorted = Path(outpath, "best_sorted.csv")
    assert best_in_context.exists()
    assert best_sorted.exists()

    # delete the files
    if delete_files:
        best_in_context.unlink()
        best_sorted.unlink()

