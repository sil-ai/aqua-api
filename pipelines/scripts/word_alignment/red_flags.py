from pathlib import Path
import argparse
import pandas as pd
from tqdm import tqdm
import math
tqdm.pandas()

import combined

def get_score(df: pd.DataFrame, vref: str, source: str):
    """
    Looks up a total_score for a particular source word at a particular vref in a dataframe.
    """
    filtered_df = df[(df['vref'] == vref) & (df['source'] == source)]
    score = 0
    if len(filtered_df) > 0:
        score = filtered_df.sort_values('total_score', ascending=False).iloc[0]['total_score']
    return score

def read_dfs(ref_df_outpaths:dict) -> dict:
    """
    Takes a dictionary of Paths to data from source text to reference texts and returns a dataframe of data for each.
    """
    ref_dfs = {}
    print('Loading reference translation data...')
    for language in tqdm(ref_df_outpaths):
        ref_dfs[language] = pd.read_csv(ref_df_outpaths[language] / 'all_in_context_with_scores.csv')
        ref_dfs[language] = ref_dfs[language].groupby(['vref', 'source', 'target']).agg({'total_score': 'max', 'simple_total': 'max'}).reset_index()
        ref_dfs[language] = ref_dfs[language].loc[ref_dfs[language].groupby(['vref', 'source'])['simple_total'].idxmax(), :]
    return ref_dfs

def identify_red_flags(outpath: Path, ref_df_outpaths:dict) -> pd.DataFrame:
    """
    Takes the directory of the source-target outputs, and a dictionary of reference language to reference language source-target outputs.
    Returns "red flags", which are source words that score low in the target language alignment data, compared to how they
    score in the source - reference language data.
    Inputs:
    outpath         Path to the source-target output files
    ref_df_outpaths Dictionary where the keys are language names, and the values are paths to source-target output files, where 
                    they key is the target language.
    Outputs:
    red_flags       A dataframe with low scores for source-target alignments, when those same source words score highly in that
                    context in the reference languages.
    """
    df = pd.read_csv(outpath / 'all_in_context_with_scores.csv')
    df.loc[:, 'order'] = df.index

    possible_red_flags = df.groupby(['vref', 'source', 'target']).agg({'simple_total': 'max', 'order': 'first'}).reset_index()
    possible_red_flags = possible_red_flags.groupby(['vref', 'source']).agg({'simple_total': 'sum', 'order': 'first'}).reset_index()
    possible_red_flags = possible_red_flags[possible_red_flags['simple_total'] < 0.1]
    possible_red_flags = possible_red_flags.sort_values('order')
    columns = ['vref', 'source', 'simple_total', 'target']

    ref_dfs = read_dfs(ref_df_outpaths)

    for language in ref_dfs:
        possible_red_flags = possible_red_flags.merge(ref_dfs[language][columns], how='left', on=['vref', 'source'], suffixes=('', f'_{language}'))
        possible_red_flags[f'simple_total_{language}'].fillna(0, inplace = True)
    score_cols = [f'simple_total_{language}' for language in ref_dfs]
    possible_red_flags.loc[:, 'avg_total_score'] = possible_red_flags[score_cols].progress_apply(lambda scores: scores.mean(), axis=1)
    possible_red_flags.loc[:, 'min_total_score'] = possible_red_flags[score_cols].progress_apply(lambda scores: scores.min(), axis=1)
    red_flags = possible_red_flags[possible_red_flags.apply(lambda row: row['avg_total_score'] > 10 * row['simple_total'] and row['min_total_score'] > 0.3, axis=1)]
    return red_flags


def main(args):
    base_outpath = args.outpath
    source = args.source
    target = args.target
    ref_df_outpaths = {}
    for reference in args.reference:
        outpath = base_outpath / f'{source.stem}_{reference.stem}'
        if args.refresh or not (outpath / 'all_in_context_with_scores.csv').exists():
            outpath.mkdir(parents=True, exist_ok=True)
            if not args.combine_only:
                combined.run_all_alignments(
                                source,
                                reference,
                                outpath,
                                is_bible=True,
                                jaccard_similarity_threshold=0.05,
                                count_threshold=0,
                                refresh_cache=True,
                )

            combined.run_combine_results(outpath)
            combined.add_scores_to_alignments(source, target, outpath, is_bible=True)
        ref_df_outpaths[reference.stem] = outpath
    outpath = base_outpath / f'{source.stem}_{target.stem}'
    if args.refresh or not (outpath / 'all_in_context_with_scores.csv').exists():
            outpath.mkdir(parents=True, exist_ok=True)
            combined.run_all_alignments(
                            source,
                            target,
                            outpath,
                            is_bible=True,
                            jaccard_similarity_threshold=0.05,
                            count_threshold=0,
                            refresh_cache=True
            )

            combined.run_combine_results(outpath)
            combined.add_scores_to_alignments(source, target, outpath, is_bible=True)
    red_flags = identify_red_flags(outpath, ref_df_outpaths)
    red_flags.to_csv(outpath / 'red_flags.csv')


if __name__ == "__main__":
    pd.options.mode.chained_assignment = None  # default='warn'
    pd.set_option("display.max_rows", 500)
    tqdm.pandas()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source",
        type=Path,
        help="Can be a Bible version string, or 'OT_domains', 'NT_domains', 'hebrew' or 'greek'",
        required=True,
    )
    parser.add_argument(
        "--target",
        type=Path,
        help="Can be a Bible version string, or 'OT_domains', 'NT_domains', 'hebrew' or 'greek'",
        required=True,
    )
    parser.add_argument(
        '--reference', 
        nargs='+', 
        type=Path,
        help="A list of texts to compare to."
        )
    parser.add_argument("--outpath", type=Path, help="Base output path, which all csv files are contained in.")
    parser.add_argument("--refresh", action='store_true', help="Refresh the data - calculate the alignments and matches again, rather than using existing csv files")
    parser.add_argument("--combine-only", action='store_true', help="Only combine the results, since the alignment and matching files already exist")
    args = parser.parse_args()
    main(args)

