from pathlib import Path
import argparse

import pandas as pd
from tqdm import tqdm
tqdm.pandas()

import combined


def identify_red_flags(outpath: Path, ref_path:Path, total_col: str='simple_total') -> pd.DataFrame:
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
    df.loc[:, 'order'] = df.index
    df['order'] = -1 * df.groupby(['vref', 'source'])['order'].transform('min')
    df.loc[:, total_col] = df[total_col].apply(lambda x: max(x, 0))
    print("Calculating best match for each source word...")
    possible_red_flags = df.loc[df.groupby(['vref', 'source'])[total_col].idxmax(), :]
    possible_red_flags.index = possible_red_flags['order']
    possible_red_flags = possible_red_flags.sort_values(['vref', 'source', total_col], ascending=False).groupby(['vref', 'source']).agg({total_col: 'sum', 'order': 'first'}).sort_values('order', ascending=False).reset_index()
    possible_red_flags = possible_red_flags[possible_red_flags[total_col] < 0.1]
    ref = pd.read_csv(ref_path)
    possible_red_flags = possible_red_flags.merge(ref, how='left', on=['vref', 'source'])
    red_flags = possible_red_flags[possible_red_flags.apply(lambda row: row['mean'] > 5 * row[total_col] and row['min'] > 0.3, axis=1)]
    # red_flags = possible_red_flags[possible_red_flags.apply(lambda row: row['avg_total_score'] > 10 * row[total_col], axis=1)]
    
    return red_flags

def main(args):
    base_outpath = args.outpath
    source = args.source
    target = args.target
    word_dict_src = None
    word_dict_trg = None
    total_col = 'simple_total' if args.exclude_encodings else 'total_score'
    outpath = base_outpath / f'{source.stem}_{target.stem}'
    if args.refresh or not (outpath / 'summary_scores.csv').exists():
        outpath.mkdir(parents=True, exist_ok=True)
        if not args.combine_only or not (outpath / 'dictionary.json').exists():
            word_dict_src, word_dict_trg = combined.run_all_alignments(
                            source,
                            target,
                            outpath,
                            word_dict_src=word_dict_src,
                            word_dict_trg=word_dict_trg,
                            is_bible=True,
                            jaccard_similarity_threshold=0.05,
                            count_threshold=0,
                            refresh_cache=args.refresh_cache,
            )

        combined.run_combine_results(outpath)
        combined.combine_by_verse_scores(source, target, outpath, word_dict_src=word_dict_src, word_dict_trg=word_dict_trg, is_bible=True)
    del word_dict_src
    del word_dict_trg
    print("Identifying red flags...")
    ref_path = Path(f'data/ref_data/{source.stem}_ref_word_scores.csv')
    red_flags = identify_red_flags(outpath, ref_path, total_col=total_col)
    red_flags.to_csv(outpath / f'red_flags.csv')


if __name__ == "__main__":
    pd.options.mode.chained_assignment = None  # default='warn'
    pd.set_option("display.max_rows", 500)
    tqdm.pandas()
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, help="Can be a Bible version string, or 'OT_domains', 'NT_domains', 'hebrew' or 'greek'", required=True)
    parser.add_argument("--target", type=Path, help="Can be a Bible version string, or 'OT_domains', 'NT_domains', 'hebrew' or 'greek'", required=True,)
    # parser.add_argument('--reference', nargs='+', type=Path, help="A list of texts to compare to.", required=True)
    parser.add_argument("--outpath", type=Path, help="Base output path, which all csv files are contained in.", required=True)
    parser.add_argument("--refresh", action='store_true', help="Refresh the data - calculate the alignments and matches again, rather than using existing csv files")
    parser.add_argument("--refresh-cache", action='store_true', help="Refresh the index cache files")
    parser.add_argument("--combine-only", action='store_true', help="Only combine the results, since the alignment and matching files already exist")
    parser.add_argument("--exclude-encodings", action='store_true', help="Exclude encoding_dist in the total scores")

    args = parser.parse_args()
    main(args)

