from pathlib import Path
import argparse
import json

import pandas as pd
from tqdm import tqdm
tqdm.pandas()

import combined


def identify_red_flags(outpath: Path, ref_path:Path) -> pd.DataFrame:
    """
    Takes the directory of the source-target outputs, and a dictionary of reference language to reference language source-target outputs.
    Returns "red flags", which are source words that score low in the target language alignment data, compared to how they
    score in the source - reference language data.
    Inputs:
    outpath         Path to the source-target output files
    ref_df_outpaths Dictionary where the keys are language names, and the values are paths to source-target output files, where 
                    the key is the target language.
    total_col               String for the column to use for scores. Normally "total_score" or "simple_total".

    Outputs:
    red_flags       A dataframe with low scores for source-target alignments, when those same source words score highly in that
                    context in the reference languages.
    """
    df = pd.read_csv(outpath / 'summary_scores.csv')
    # df.loc[:, 'order'] = df.index
    # df['order'] = -1 * df.groupby(['vref', 'source'])['order'].transform('min')
    df.loc[:, 'total_score'] = df['total_score'].apply(lambda x: max(x, 0))
    print("Calculating best match for each source word...")
    possible_red_flags = df.loc[df.groupby(['vref', 'source'], sort=False)['total_score'].idxmax(), :].reset_index(drop=True)
    # possible_red_flags.index = possible_red_flags['order']
    # possible_red_flags = possible_red_flags.sort_values(['vref', 'source', total_col], ascending=False).groupby(['vref', 'source'], sort=False).agg({total_col: 'sum'}).reset_index()
    possible_red_flags = possible_red_flags.loc[:, ['vref', 'source', 'total_score']]
    possible_red_flags = possible_red_flags[possible_red_flags['total_score'] < 0.1]
    possible_red_flags.to_csv(outpath / 'possible_red_flags.csv')
    if ref_path.exists():
        ref = pd.read_csv(ref_path, low_memory=False)
        possible_red_flags = possible_red_flags.merge(ref, how='left', on=['vref', 'source'], sort=False)
        red_flags = possible_red_flags[possible_red_flags.apply(lambda row: row['mean'] > 5 * row['total_score'] and row['min'] > 0.3, axis=1)]
    else:
        red_flags = possible_red_flags
    # red_flags = possible_red_flags[possible_red_flags.apply(lambda row: row['avg_total_score'] > 10 * row[total_col], axis=1)]
    
    return red_flags

def main(args):
    base_outpath = args.outpath
    base_inpath = args.inpath
    base_ref_inpath = args.ref_inpath
    sources = []
    targets = []
    for ref_dir in base_inpath.iterdir():
        meta_file = ref_dir / 'meta.json'
        with open(meta_file) as f:
            meta = json.load(f)
        source_str = meta['source']
        target_str = meta['target']
        sources.append(source_str)
        targets.append(target_str)
    
    for source_str in sources:
        for target_str in targets:
            inpath = base_inpath / f'{source_str}_{target_str}'
            outpath = base_outpath / f'{source_str}_{target_str}'
    
            print(f"Identifying red flags for {source_str} to {target_str}...")
            ref_path = base_ref_inpath / f'{source_str}/{source_str}_ref_word_scores.csv'
            red_flags = identify_red_flags(inpath, ref_path)
            red_flags.to_csv(outpath / f'red_flags.csv', index=False)


if __name__ == "__main__":
    tqdm.pandas()
    parser = argparse.ArgumentParser()
    parser.add_argument('--references', nargs='+', type=Path, help="A list of texts to compare to.", required=True)
    parser.add_argument("--inpath", type=Path, help="Base input path, which all csv files are contained in.", required=True)
    parser.add_argument("--ref-inpath", type=Path, help="Base input path for reference csv files.", required=True)
    parser.add_argument("--outpath", type=Path, help="Base output path, to write to.", required=True)

    args = parser.parse_args()
    main(args)

