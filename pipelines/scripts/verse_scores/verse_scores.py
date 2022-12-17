from pathlib import Path
import json
import argparse

import pandas as pd


def get_verse_scores(total_scores: pd.DataFrame):
    verse_scores = total_scores.groupby('vref', sort=False).mean()
    verse_scores = verse_scores.fillna(0)
    return verse_scores
    
    
def main(args):
    for top_source_scores_dir in args.top_source_scores_dir.iterdir():
        print(top_source_scores_dir)
        meta_file = top_source_scores_dir / 'meta.json'
        with open(meta_file) as f:
            meta = json.load(f)
        source_str = meta['source']
        target_str = meta['target']
        outpath = args.outpath / f'{source_str}_{target_str}'
        top_source_scores = pd.read_csv(top_source_scores_dir / 'top_source_scores.csv')
        verse_scores = get_verse_scores(top_source_scores)
        verse_scores.to_csv(outpath / 'verse_scores.csv')
    with open(args.outpath / 'meta.json', 'w') as f:
                json.dump(meta, f)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-source-scores-dir", type=Path, help="directory with top source scores")
    parser.add_argument("--outpath", type=Path, default=Path("/pfs/out"), help="Output directory")
    args = parser.parse_args()

    main(args) 