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
    score = df[(df['vref'] == vref) & (df['source'] == source)].sort_values('total_score', ascending=False).iloc[0]['total_score']
    return score

def identify_red_flags(outpath: Path, ref_df_outpaths:dict):
    df = pd.read_csv(outpath / 'all_in_context_with_scores.csv')
    for language in ref_df_outpaths:
        ref_df_outpaths[language] = pd.read_csv(ref_df_outpaths[language])
    concerns = df.groupby(['vref', 'source']).agg({'total_score': 'sum', 'target': 'first'}).sort_values('total_score')
    concerns = concerns[concerns['total_score'] < 0.05]
    language_columns = [f'{key}_score' for key in ref_df_outpaths.keys()]
    red_flags = pd.DataFrame(columns=['vref', 'source', 'total_score', *language_columns])
    print(concerns)
    for index, row in tqdm(concerns.iterrows(), total=concerns.shape[0]):
        vref = index[0]
        source = index[1]
        if row['target'] == 'range':
            continue
        scores = {}
        for language in ref_df_outpaths:
            scores[language] = get_score(ref_df_outpaths[language], vref, source)
        score_values = list(scores.values())
        if min(score_values) > 0.4 and sum(score_values) / len(score_values) > row['total_score'] * 25:
            new_row = [vref, source, row['total_score'], *score_values]
            red_flags.loc[len(red_flags)] = new_row
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

