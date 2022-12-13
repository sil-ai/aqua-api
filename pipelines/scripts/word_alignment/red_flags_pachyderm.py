from pathlib import Path
import argparse
import json

import pandas as pd
from tqdm import tqdm
tqdm.pandas()


def identify_red_flags(outpath: Path, ref_path:Path) -> pd.DataFrame:
    """
    Takes the directory of the source-target outputs, and a dictionary of reference language to reference language source-target outputs.
    Returns "red flags", which are source words that score low in the target language alignment data, compared to how they
    score in the source - reference language data.
    Inputs:
    outpath         Path to the source-target output files
    ref_path        Path ti reference csv file

    Outputs:
    red_flags       A dataframe with low scores for source-target alignments, when those same source words score highly in that
                    context in the reference languages.
    """
    df = pd.read_csv(outpath / 'word_scores.csv')
    df = df.loc[:, ['vref', 'source', 'total_score']]
    df.loc[:, 'total_score'] = df['total_score'].apply(lambda x: max(x, 0))
    df.loc[:, 'total_score'] = df['total_score'].fillna(0)
    possible_red_flags = df[df['total_score'] < 0.1]
    possible_red_flags.to_csv(outpath / 'possible_red_flags.csv')
    if not ref_path.exists():
        return possible_red_flags, possible_red_flags

    ref = pd.read_csv(ref_path, low_memory=False)
    if 'mean' not in ref.columns:
        return possible_red_flags
    possible_red_flags = possible_red_flags.merge(ref, how='left', on=['vref', 'source'], sort=False)
    red_flags = possible_red_flags[possible_red_flags.apply(lambda row: row['mean'] > 5 * row['total_score'] and row['mean'] > 0.35, axis=1)]
    
    return possible_red_flags, red_flags


def get_latest_ref_file(base_ref_inpath, source_str):
    file_str = '_ref_word_scores'
    ref_dir = base_ref_inpath / source_str
    files = []
    for file in ref_dir.iterdir():
        if file_str in file.name:
            files.append(file.name)
    files = sorted(files)
    latest_ref = ref_dir / files[-1]
    return latest_ref


def main(args):
    base_outpath = args.outpath
    base_inpath = args.inpath
    base_ref_inpath = args.ref_inpath
    for ref_dir in base_inpath.iterdir():  
        meta_file = ref_dir / 'meta.json'
        with open(meta_file) as f:
            meta = json.load(f)
        source_str = meta['source']
        target_str = meta['target']
        print(f'Source: {source_str}')
        print(f'Target: {target_str}')
    
        inpath = base_inpath / f'{source_str}_{target_str}'
        outpath = base_outpath / f'{source_str}_{target_str}'
        if not outpath.exists():
            outpath.mkdir()

        print(f"Identifying red flags for {source_str} to {target_str}...")
        ref_path = get_latest_ref_file(base_ref_inpath, source_str)
        possible_red_flags, red_flags = identify_red_flags(inpath, ref_path)
        red_flags.to_csv(outpath / f'red_flags.csv', index=False)
        possible_red_flags.to_csv(outpath / f'possible_red_flags.csv', index=False)



if __name__ == "__main__":
    tqdm.pandas()
    parser = argparse.ArgumentParser()
    parser.add_argument("--inpath", type=Path, help="Base input path, which all csv files are contained in.", required=True)
    parser.add_argument("--ref-inpath", type=Path, help="Base input path for reference csv files.", required=True)
    parser.add_argument("--outpath", type=Path, help="Base output path, to write to.", required=True)

    args = parser.parse_args()
    main(args)

