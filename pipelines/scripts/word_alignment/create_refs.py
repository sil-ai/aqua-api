from pathlib import Path
import argparse
import json
from typing import List, Tuple

import pandas as pd

import get_data


def get_scores(data_path: Path):
    verse_scores = pd.read_csv(data_path / 'verse_scores.csv')
    word_scores = pd.read_csv(data_path / 'word_scores.csv')
    if verse_scores.shape[0] > 0:
        verse_scores = verse_scores[['vref', 'total_score']]
        word_scores = word_scores[['vref', 'source', 'target', 'total_score']]
        word_scores['target'] = word_scores['target'].apply(lambda x: x.replace(';', '";"')) # Otherwise the separation gets messed up in the CSV file
    return (word_scores, verse_scores)

def add_ref_scores(
                verse_df: pd.DataFrame, 
                word_df: pd.DataFrame, 
                source_str: str, 
                target_str: str, 
                base_inpath: Path,
                ) -> Tuple[pd.DataFrame, pd.DataFrame]:
    inpath = base_inpath / f"{source_str}_{target_str}" 
    if not inpath.exists():
        print(f'{inpath} does not exist, skipping.')
        return (verse_df, word_df)
    if target_str in word_df.columns and target_str in verse_df.columns:  # Skip if already in the df
        return (verse_df, word_df)

    word_scores, verse_scores = get_scores(inpath)
    if verse_scores.shape[0] == 0:
        return (verse_df, word_df)
    verse_df = verse_df.merge(verse_scores, how='left', on='vref').rename(columns={'total_score': target_str})
    word_df = word_df.merge(word_scores, how='left', on=['vref', 'source']).rename(columns={'total_score': f'{target_str}_score', 'target': f'{target_str}_match'})
    references  = [col for col in verse_df.columns if col not in ['vref', 'mean']]
    
    if len(references) > 0:
        verse_df['mean'] = verse_df.loc[:, references].mean(axis=1)
        word_df['mean'] = word_df.loc[:, [f'{reference}_score' for reference in references]].mean(axis=1)
        verse_df['min'] = verse_df.loc[:, references].min(axis=1)
        word_df['min'] = word_df.loc[:, [f'{reference}_score' for reference in references]].min(axis=1)
    print(verse_df)
    print(word_df)
    if len(references) > 1:
        verse_df['second_min'] = verse_df.loc[:, references].apply(lambda row: sorted(list(row))[1], axis=1)
        word_df['second_min'] = word_df.loc[:, [f'{reference}_score' for reference in references]].apply(lambda row: sorted(list(row))[1], axis=1)
    return verse_df, word_df


def get_source_txt(base_inpath, source_str, target_str):
    source_path = base_inpath / f'{source_str}_{target_str}/{source_str}.txt'
    if source_path.exists():
        return source_path
    return None


def main(args):
    base_inpath = args.inpath
    base_ref_inpath = args.inpath
    base_outpath = args.outpath
    tmp_outpath = Path('/tmp/refs')
    if not tmp_outpath.exists():
        tmp_outpath.mkdir()
    sources = set()
    for ref_dir in base_inpath.iterdir(): # FYI, this should only be one ref_dir per pachyderm datum
        meta_file = ref_dir / 'meta.json'
        with open(meta_file) as f:
            meta = json.load(f)
        source_str = meta['source']
        target_str = meta['target']
        sources.add(source_str)
    sources = list(sources)  # Should only have one element
    print(f'Sources: {sources}')
    print(f'Target: {target_str}')    
            
    for source_str in sources:
        if not (base_outpath / f'{source_str}').exists():
            (base_outpath / f'{source_str}').mkdir(parents=True)
        if not (tmp_outpath / f'{source_str}').exists():
            (tmp_outpath / f'{source_str}').mkdir(parents=True)
        source = get_source_txt(base_inpath, source_str, target_str)
        if source == None:
            continue
        if ((tmp_outpath / f'{source_str}/{source_str}_all_ref_verse_scores.csv').exists()
            and (tmp_outpath / f'{source_str}/{source_str}_all_ref_word_scores.csv').exists()
            ):
            # We must bepart-way through a run, so use the files in the outpath
            all_ref_verse_df = pd.read_csv(tmp_outpath / f'{source_str}/{source_str}_all_ref_verse_scores.csv')
            all_ref_word_df = pd.read_csv(tmp_outpath / f'{source_str}/{source_str}_all_ref_word_scores.csv')
        elif ((base_ref_inpath / f'{source_str}/{source_str}_all_ref_verse_scores.csv').exists()
            and (base_ref_inpath / f'{source_str}/{source_str}_all_ref_word_scores.csv').exists()
            and not args.refresh
            ):
            # Use the files from the base_ref_inpath
            all_ref_verse_df = pd.read_csv(base_ref_inpath / f'{source_str}/{source_str}_all_ref_verse_scores.csv')
            all_ref_word_df = pd.read_csv(base_ref_inpath / f'{source_str}/{source_str}_all_ref_word_scores.csv')
        else:

            df = get_data.get_ref_df(source, is_bible=True)
            df = get_data.remove_blanks_and_ranges(df)
            all_ref_verse_df = df.drop('src', axis=1)
            
            df = get_data.get_words_from_txt_file(df, base_outpath)
            all_ref_word_df = df.explode('src_words')[['vref', 'src_words']].rename(columns={'src_words': 'source'})

        print(all_ref_verse_df)
        print(all_ref_word_df)
        all_ref_verse_df, all_ref_word_df = add_ref_scores(all_ref_verse_df, all_ref_word_df, source_str, target_str, base_inpath)
        all_ref_verse_df.to_csv(base_outpath / f'{source_str}/{source_str}_all_ref_verse_scores.csv', index=False)
        all_ref_word_df.to_csv(base_outpath / f'{source_str}/{source_str}_all_ref_word_scores.csv', index=False)
        all_ref_verse_df.to_csv(tmp_outpath / f'{source_str}/{source_str}_all_ref_verse_scores.csv', index=False)
        all_ref_word_df.to_csv(tmp_outpath / f'{source_str}/{source_str}_all_ref_word_scores.csv', index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--inpath", type=Path, help="Path to base directory where scores are saved", required=True)
    parser.add_argument("--ref-inpath", type=Path, help="Path to base directory where the ref summary csvs are saved", required=True)
    parser.add_argument("--outpath", type=Path, help="Path to base outpath directory", required=True)
    parser.add_argument("--refresh", action="store_true", help="Refresh the csv files, building them from scratch")

    args = parser.parse_args()
    main(args)
