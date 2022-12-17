from pathlib import Path
import pytest
import pandas as pd
import get_data, alignment_scores, train_fa_model

@pytest.mark.parametrize("source,target,is_bible", [
                                            (Path("fixtures/in/de-LU1912-mini/de-LU1912-mini.txt"), Path("fixtures/in/en-KJV-mini/en-KJV-mini.txt"), True), 
                                            (Path("fixtures/in/en-KJV-mini/en-KJV-mini.txt"), Path("fixtures/in/es-NTV-mini/es-NTV-mini.txt"), True),
                                            (Path("fixtures/in/es-NTV-mini/es-NTV-mini.txt"), Path("fixtures/in/de-LU1912-mini/de-LU1912-mini.txt"), True) 
                                            ])
def test_get_alignments(source, target, is_bible):
    outpath =  Path('fixtures/out') / f'{source.stem}_{target.stem}'
    outpath.mkdir(exist_ok=True, parents=True)
    ref_df = get_data.get_ref_df(source, target, is_bible=is_bible)
    df = get_data.condense_files(ref_df)
    vrefs = list(df['vref'])
    condensed_source, condensed_target = get_data.write_condensed_files(df, outpath)
    corpus = get_data.create_corpus(condensed_source, condensed_target)
    model = train_fa_model.train_model(corpus)
    alignments = alignment_scores.get_best_alignment_scores(model, corpus, vrefs)

    assert type(alignments) == pd.DataFrame
    assert len(alignments) > 0
    assert len(alignments['alignment_score'].unique()) > 10
    assert len(alignments['source'].unique()) > 10
    assert len(alignments['target'].unique()) > 10
    assert len(alignments['alignment_count'].unique()) == 1
    condensed_source.unlink()
    condensed_target.unlink()


@pytest.mark.parametrize("source,target,is_bible", [
                                            (Path("fixtures/in/de-LU1912-mini/de-LU1912-mini.txt"), Path("fixtures/in/en-KJV-mini/en-KJV-mini.txt"), True), 
                                            (Path("fixtures/in/en-KJV-mini/en-KJV-mini.txt"), Path("fixtures/in/es-NTV-mini/es-NTV-mini.txt"), True),
                                            (Path("fixtures/in/es-NTV-mini/es-NTV-mini.txt"), Path("fixtures/in/de-LU1912-mini/de-LU1912-mini.txt"), True) 
                                            ])
def test_get_best_alignment_scores(source, target, is_bible):
    outpath =  Path('fixtures/out') / f'{source.stem}_{target.stem}'
    outpath.mkdir(exist_ok=True, parents=True)
    ref_df = get_data.get_ref_df(source, target, is_bible=is_bible)
    df = get_data.condense_files(ref_df)
    vrefs = list(df['vref'])
    condensed_source, condensed_target = get_data.write_condensed_files(df, outpath)
    corpus = get_data.create_corpus(condensed_source, condensed_target)
    model = train_fa_model.train_model(corpus)
    df = alignment_scores.get_best_alignment_scores(model, corpus, vrefs)
    assert len(df) > 20
    assert len(df['alignment_score'].unique()) > 20

    df.to_csv(outpath / 'best_alignment_scores.csv')

    condensed_source.unlink()
    condensed_target.unlink()


@pytest.mark.parametrize("source,target,is_bible", [
                                            (Path("fixtures/in/de-LU1912-mini/de-LU1912-mini.txt"), Path("fixtures/in/en-KJV-mini/en-KJV-mini.txt"), True), 
                                            (Path("fixtures/in/en-KJV-mini/en-KJV-mini.txt"), Path("fixtures/in/es-NTV-mini/es-NTV-mini.txt"), True),
                                            (Path("fixtures/in/es-NTV-mini/es-NTV-mini.txt"), Path("fixtures/in/de-LU1912-mini/de-LU1912-mini.txt"), True) 
                                            ])
def test_average_scores(source, target, is_bible):
    outpath =  Path('fixtures/out') / f'{source.stem}_{target.stem}'
    df = pd.read_csv(outpath / 'best_alignment_scores.csv')
    avg_df = alignment_scores.average_scores(df)
    assert isinstance(avg_df, pd.DataFrame)
    assert avg_df.shape[0] > 0
    assert avg_df['alignment_count'].sum() > 0
    avg_df.to_csv(outpath / 'avg_df.csv')


@pytest.mark.parametrize("source,target,is_bible", [
                                            (Path("fixtures/in/de-LU1912-mini/de-LU1912-mini.txt"), Path("fixtures/in/en-KJV-mini/en-KJV-mini.txt"), True), 
                                            (Path("fixtures/in/en-KJV-mini/en-KJV-mini.txt"), Path("fixtures/in/es-NTV-mini/es-NTV-mini.txt"), True),
                                            (Path("fixtures/in/es-NTV-mini/es-NTV-mini.txt"), Path("fixtures/in/de-LU1912-mini/de-LU1912-mini.txt"), True) 
                                            ])
def test_save_alignment_scores(source, target, is_bible):
    outpath =  Path('fixtures/out') / f'{source.stem}_{target.stem}'
    df = pd.read_csv(outpath / 'best_alignment_scores.csv')
    avg_df = pd.read_csv(outpath / 'avg_df.csv')
    alignment_scores.save_alignment_scores(avg_df, outpath / "avg_alignment_scores.csv")
    alignment_scores.save_alignment_scores(df, outpath / "alignment_scores.csv")
    (outpath / 'best_alignment_scores.csv').unlink()
    (outpath / 'avg_df.csv').unlink()



@pytest.mark.parametrize("source_dir,is_bible", [
                                            (Path("fixtures/in/de-LU1912-mini/"), True), 
                                            (Path("fixtures/in/en-KJV-mini/"), True),
                                            (Path("fixtures/in/es-NTV-mini/"), True) 
                                            ])
def test_get_source(source_dir: Path, is_bible:bool):
    source, source_str = alignment_scores.get_source(source_dir)
    with open(source) as f:
        lines = f.readlines()
    if is_bible:
        assert len(lines) == 41899
    assert len(lines) > 0
    assert isinstance(source_str, str)
    assert source.stem == source_str



@pytest.mark.parametrize("source,target,is_bible", [
                                            (Path("fixtures/in/de-LU1912-mini/de-LU1912-mini.txt"), Path("fixtures/in/en-KJV-mini/en-KJV-mini.txt"), True), 
                                            (Path("fixtures/in/en-KJV-mini/en-KJV-mini.txt"), Path("fixtures/in/es-NTV-mini/es-NTV-mini.txt"), True),
                                            (Path("fixtures/in/es-NTV-mini/es-NTV-mini.txt"), Path("fixtures/in/de-LU1912-mini/de-LU1912-mini.txt"), True) 
                                            ])
def test_run_best_align(source, target, is_bible):
    outpath =  Path('fixtures/out') / f'{source.stem}_{target.stem}'
    alignment_scores.run_best_align(source, target, outpath, is_bible=is_bible)

    # check forward files exist
    alignment_scores_df = Path(outpath, "alignment_scores.csv")
    avg_alignment_scores_df = Path(outpath, "avg_alignment_scores.csv")
    assert alignment_scores_df.exists()
    assert avg_alignment_scores_df.exists()
