from pathlib import Path
import argparse
import json
from typing import List, Tuple

import pandas as pd

import get_data


def get_scores(outpath: Path):
    verse_scores = pd.read_csv(outpath / 'verse_scores.csv')
    word_scores = pd.read_csv(outpath / 'word_scores.csv')
    if verse_scores.shape[0] == 0:
        return (None, None)
    verse_scores = verse_scores[['vref', 'total_score']]
    word_scores = word_scores[['vref', 'source', 'target', 'total_score']]
    word_scores['target'] = word_scores['target'].apply(lambda x: x.replace(';', '";"')) # Otherwise the separation gets messed up in the CSV file
    return (word_scores, verse_scores)

def get_ref_scores(
                references: List[str], 
                verse_df: pd.DataFrame, 
                word_df: pd.DataFrame, 
                source: str, 
                base_outpath: Path
                ) -> Tuple[pd.DataFrame, pd.DataFrame]:
    references = [reference for reference in references if (base_outpath / f"{source}_{reference}").exists()]
    null_references = []
    for reference in references:
        outpath = base_outpath / f"{source}_{reference}" 
        word_scores, verse_scores = get_scores(outpath)
        if verse_scores == None:
            null_references.append(reference)
            continue
        verse_df = verse_df.merge(verse_scores, how='left', on='vref').rename(columns={'total_score': reference})
        word_df = word_df.merge(word_scores, how='left', on=['vref', 'source']).rename(columns={'total_score': f'{reference}_score', 'target': f'{reference}_match'})
    references  = [reference for reference in references if reference not in null_references]
    print([f'{reference}_score' for reference in references])
    
    if len(references) > 0:
        verse_df['mean'] = verse_df.loc[:, references].mean(axis=1)
        word_df['mean'] = word_df.loc[:, [f'{reference}_score' for reference in references]].mean(axis=1)
        verse_df['min'] = verse_df.loc[:, references].min(axis=1)
        word_df['min'] = word_df.loc[:, [f'{reference}_score' for reference in references]].min(axis=1)
    print(verse_df)
    if len(references) > 1:
        verse_df['second_min'] = verse_df.loc[:, references].apply(lambda row: sorted(list(row))[1], axis=1)
        word_df['second_min'] = word_df.loc[:, [f'{reference}_score' for reference in references]].apply(lambda row: sorted(list(row))[1], axis=1)
    return verse_df, word_df


def main(args):
    base_inpath = args.inpath
    base_outpath = args.outpath
    references = args.references
    sources = []
    all_references = []
    for ref_dir in base_inpath.iterdir():
        meta_file = ref_dir / 'meta.json'
        with open(meta_file) as f:
            meta = json.load(f)
        source_str = meta['source']
        target_str = meta['target']
        sources.append(source_str)
        all_references.append(target_str)
    print(references)
    print(all_references)
        
            
    for source_str in sources:
        source = base_inpath / f'{source_str}_{all_references[0]}/{source_str}.txt'
        df = get_data.get_ref_df(source, is_bible=True)
        df = get_data.remove_blanks_and_ranges(df)
        verse_df = df.drop('src', axis=1)
        df = get_data.get_words_from_txt_file(df, base_outpath)
        word_df = df.explode('src_words')[['vref', 'src_words']].rename(columns={'src_words': 'source'})
        
        ref_verse_df, ref_word_df = get_ref_scores(all_references, verse_df, word_df, source_str, base_outpath)

        if not (base_outpath / f'{source_str}').exists():
            (base_outpath / f'{source_str}').mkdir()
        ref_verse_df.to_csv(f'/pfs/out/{source_str}/{source_str}_all_ref_verse_scores.csv', index=False)
        ref_word_df.to_csv(f'/pfs/out/{source_str}/{source_str}_all_ref_word_scores.csv', index=False)
        ref_verse_df, ref_word_df = get_ref_scores(references, verse_df, word_df, source_str, base_outpath)
        ref_verse_df.to_csv(f'/pfs/out/{source_str}/{source_str}_ref_verse_scores.csv', index=False)
        ref_word_df.to_csv(f'/pfs/out/{source_str}/{source_str}_ref_word_scores.csv', index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--inpath", type=Path, help="Path to base directory where scores are saved", required=True)
    parser.add_argument("--outpath", type=Path, help="Path to base outpath directory", required=True)
    parser.add_argument("--references", type=str, nargs='*', help="List of references to be used in red flags, etc", required=True)
    args = parser.parse_args()
    main(args)