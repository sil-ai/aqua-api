from pathlib import Path
import json
import argparse

import pandas as pd

import get_data


def create_new_ref_df(source):
    df = get_data.get_ref_df(source, is_bible=True)
    df = get_data.remove_blanks_and_ranges(df)
    df = get_data.get_words_from_txt_file(df, Path('/tmp'))
    return df


def add_scores_to_ref(target_str: str, top_source_scores: pd.DataFrame, summary_top_source_scores: pd.DataFrame):
    summary_top_source_scores = summary_top_source_scores.merge(top_source_scores, how='left', on=['vref', 'source']).rename(columns={'total_score': target_str})
    
    
def main(args):
    summary_top_source_scores = None
    for source in args.sources_dir.iterdir():
        print(f'Source: {source.stem}')
        outpath = args.outpath / f'{source_str}'
        summary_top_source_scores = create_new_ref_df(source)

        for top_source_scores_dir in args.top_source_scores_dir.iterdir():
            print(top_source_scores_dir)
            meta_file = top_source_scores_dir / 'meta.json'
            with open(meta_file) as f:
                meta = json.load(f)
            source_str = meta['source']
            target_str = meta['target']
            if source_str != source.stem:
                continue
            outpath = args.outpath / f'{source_str}'
            top_source_scores = pd.read_csv(top_source_scores_dir / 'top_source_scores.csv')
            summary_top_source_scores = add_scores_to_ref(target_str, top_source_scores, summary_top_source_scores)
        summary_top_source_scores.to_csv(outpath / 'summary_top_source_scores.csv')
        source_meta = {'source': source_str}
        with open(outpath / 'meta.json', 'w') as f:
            json.dump(source_meta, f)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-source-scores-dir", type=Path, help="directory with top source scores")
    parser.add_argument("--sources-dir", type=Path, help="directory with the original source txt files")
    parser.add_argument("--outpath", type=Path, default=Path("/pfs/out"), help="Output directory")
    args = parser.parse_args()

    main(args) 