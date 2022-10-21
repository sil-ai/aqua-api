from pathlib import Path
import argparse
import pandas as pd
from tqdm import tqdm
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

def identify_red_flags(outpath: Path, ref_df_outpaths:dict):
    df = pd.read_csv(outpath / 'all_in_context_with_scores.csv')
    df.loc[:, 'order'] = df.index

    ref_dfs = {}
    print('Loading reference translation data...')
    for language in tqdm(ref_df_outpaths):
        ref_dfs[language] = pd.read_csv(ref_df_outpaths[language])
        ref_dfs[language] = ref_dfs[language].groupby(['vref', 'source', 'target']).agg({'total_score': 'max'}).reset_index()
        ref_dfs[language] = ref_dfs[language].loc[ref_dfs[language].groupby(['vref', 'source'])['total_score'].idxmax(), :]
    possible_red_flags = df.groupby(['vref', 'source', 'target']).agg({'total_score': 'max', 'order': 'first'}).reset_index()
    possible_red_flags = possible_red_flags.groupby(['vref', 'source']).agg({'total_score': 'sum', 'order': 'first'}).reset_index()
    possible_red_flags = possible_red_flags[possible_red_flags['total_score'] < 0.1]
    possible_red_flags = possible_red_flags.sort_values('order')
    columns = ['vref', 'source', 'total_score', 'target']
    for language in ref_dfs:
        possible_red_flags = possible_red_flags.merge(ref_dfs[language][columns], how='left', on=['vref', 'source'], suffixes=('', f'_{language}'))
        possible_red_flags[f'total_score_{language}'].fillna(0, inplace = True)
    score_cols = [f'total_score_{language}' for language in ref_dfs]
    possible_red_flags.loc[:, 'avg_total_score'] = possible_red_flags[score_cols].progress_apply(lambda scores: scores.mean(), axis=1)
    possible_red_flags.loc[:, 'min_total_score'] = possible_red_flags[score_cols].progress_apply(lambda scores: scores.min(), axis=1)
    red_flags = possible_red_flags[possible_red_flags.apply(lambda row: row['avg_total_score'] > 10 * row['total_score'] and row['min_total_score'] > 0.3, axis=1)]
    # possible_red_flags = df.groupby(['vref', 'source', 'target']).agg({'total_score': 'mean'}).reset_index()
    # possible_red_flags = possible_red_flags.groupby(['vref', 'source']).agg({'total_score': 'sum'}).reset_index()
    # possible_red_flags = possible_red_flags[possible_red_flags['total_score'] < 0.1]
    # possible_red_flags = possible_red_flags.sort_values('order')
    # vrefs = possible_red_flags['vref'].unique()
    # sources = possible_red_flags['source'].unique()
    # for df in ref_dfs.values():
    #     df = df[df['vref'].progress_apply(lambda x: x in vrefs)]
    #     df = df[df['source'].progress_apply(lambda x: x in sources)]

    #     # df = df[df.progress_apply(lambda row: row['vref'] in vrefs and row['source'] in sources, axis=1)]
    # language_columns = [f'{key}_score' for key in ref_dfs.keys()]
    # red_flags = pd.DataFrame(columns=['vref', 'source', 'total_score', *language_columns])
    # print(possible_red_flags)
    # for _, row in tqdm(possible_red_flags.iterrows(), total=possible_red_flags.shape[0]):
    #     vref = row['vref']
    #     source = row['source']
    #     # if row['target'] == 'range':
    #         # continue
    #     scores = {}
    #     for language in ref_dfs:
    #         scores[language] = get_score(ref_dfs[language], vref, source)
    #     score_values = list(scores.values())
    #     if min(score_values) > 0.4 and sum(score_values) / len(score_values) > row['total_score'] * 10:
    #         new_row = [vref, source, row['total_score'], *score_values]
    #         red_flags.loc[len(red_flags)] = new_row
    return red_flags


def main(args):
    base_outpath = args.outpath
    source = args.source
    target = args.target
    ref_df_outpaths = {}
    for reference in args.reference:
        outpath = base_outpath / f'{source.stem}_{reference.stem}'
        if not (outpath / 'all_in_context_with_scores.csv').exists():
            outpath.mkdir(parents=True, exist_ok=True)
            combined.run_all_alignments(
                            source,
                            reference,
                            outpath,
                            is_bible=True,
                            jaccard_similarity_threshold=0.05,
                            count_threshold=0,
            )

            combined.run_combine_results(outpath)
            combined.add_scores_to_alignments(outpath)
        ref_df_outpaths[reference.stem] = outpath / 'all_in_context_with_scores.csv'
    outpath = base_outpath / f'{source.stem}_{target.stem}'
    if not (outpath / 'all_in_context_with_scores.csv').exists():
            outpath.mkdir(parents=True, exist_ok=True)
            combined.run_all_alignments(
                            source,
                            target,
                            outpath,
                            is_bible=True,
                            jaccard_similarity_threshold=0.05,
                            count_threshold=0,
            )

            combined.run_combine_results(outpath)
            combined.add_scores_to_alignments(outpath)
    red_flags = identify_red_flags(outpath, ref_df_outpaths)
    print(red_flags)
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

    args = parser.parse_args()
    main(args)

