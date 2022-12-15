# imports
import argparse
from typing import List, Tuple, Optional
import json

import pandas as pd
from machine.corpora import TextFileTextCorpus
from machine.tokenization import LatinWordTokenizer
from machine.translation import SymmetrizationHeuristic
import get_data, train_fa_model

from machine.translation.thot import (
    ThotFastAlignWordAlignmentModel,
    ThotSymmetrizedWordAlignmentModel,
)
from pathlib import Path


def get_best_alignment_scores(
                                model: ThotSymmetrizedWordAlignmentModel, 
                                corpus: TextFileTextCorpus, 
                                vrefs: Optional[List[str]] = None
                                ) -> pd.DataFrame:
    """
    Takes a corpus and a word alignment model and calculates word alignments for each aligned line.
    Returns a dataframe with words that have been aligned by the model.

    Inputs:
    model           A machine.translation.thot.ThotSymmetrizedWordAlignmentModel
    corpus          A machine.corpora.TextFileTextCorpus of aligned texts
    vrefs           An optional list of verse references for each aligned line

    Outputs:
    df                      A dataframe, where each row is an alignment of a source word and a target word.
        source              The source word
        target              The target word
        alignment_count     Integer 1, to later be summed as a count
        verse_score         The average alignment score for the line in question
        vref                The verse reference for that line,     if a vref file has been supplied.
    """
    data = {"vref": [], "source": [], "target": [], "alignment_count": [], "alignment_score": []}
    segments = list(corpus.lowercase().to_tuples())
    alignments = model.align_batch(segments)
    c = 0
    # for source_segments, target_segments in batch(segments, model.batch_size):

    for (source_segment, target_segment), alignment in zip(list(segments), alignments):
        word_pairs = alignment.to_aligned_word_pairs()
        model.compute_aligned_word_pair_scores(source_segment, target_segment, word_pairs)

        vref = vrefs[c] if vrefs else None
        c = c + 1
        for pair in word_pairs:
            data["source"].append(source_segment[pair.source_index])
            data["target"].append(target_segment[pair.target_index])
            data["alignment_count"] = 1
            data["alignment_score"].append(pair.alignment_score)
            data["vref"].append(vref)

    df = pd.DataFrame(data)
    return df


def get_vref_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes a dataframe that includes both "vref" and "verse_score" columns, and returns
    a dataframe of just those two columns.

    Inputs:
    df      A dataframe with both "vref" and "verse_score" columns

    Outputs:
    vref_df     A dataframe with just "vref" and "verse_score" columns
    """
    # remove duplicate verses
    df = df.drop_duplicates(subset=["vref"])
    vref_df = df[["vref", "verse_score"]]
    return vref_df

def average_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes a dataframe of aligned matches, removes duplicate source word / target word combinations,
    returns the dataframe.

    Inputs:
    df          A dataframe of alignment matches

    Outputs:
    no_dups     A dataframe with duplicates removed, grouped by source and target words.
    """
    # remove duplicates and average out verse and word scores
    dups = df.groupby(["source", "target"]).size().reset_index()
    avgs = df[['source', 'target', 'alignment_count', 'alignment_score']].groupby(["source", "target"]).mean(numeric_only=True).reset_index()
    no_dups = pd.merge(dups, avgs)
    no_dups.drop(columns=['alignment_count'], inplace=True)
    no_dups.rename(columns={0: "alignment_count"}, inplace=True)
    return no_dups


def run_best_align(
                    source: Path, 
                    target: Path, 
                    outpath: Path, 
                    is_bible: bool=False, 
                    parallel_corpus: TextFileTextCorpus = None, 
                    symmetrized_model: ThotSymmetrizedWordAlignmentModel = None,
                    ) -> Tuple[TextFileTextCorpus, ThotSymmetrizedWordAlignmentModel]:
    """
    Takes two input text files, runs get_alignments on them, and saves the resulting dataframe
    to a csv file in a directory within outpath.

    Inputs:
    source           Path to a source text file of line-aligned text
    target           Path to a target text file of line-aligned text
    outpath            Path to base output directory
    is_bible           Boolean for whether the text is Bible, and hence vref references should be used.
    parallel_corpus    A corpus to process. Normally the corpus is produced from the source and target,
                        but if it has already been produced it can optionally be provided here to save
                        calculating it again.
    symmetrized_model   The model to use. Normally the model is instantiated and trained with the source and target,
                        but if it has already been created and trained it can optionally be provided here to save
                        training it again.

    Outputs:
    TextFileTextCorpus      In case you want to re-use it without creating it from scratch
    ThotSymmetrizedWordAlignmentModel       In case you want to re-use it without training from scratch
    """
    # get vrefs
    ref_df = get_data.get_ref_df(source, target, is_bible)
    df = get_data.condense_files(ref_df)
    vrefs = list(df['vref'])
    condensed_source, condensed_target = get_data.write_condensed_files(df, outpath)
    
    # create parallel corpus
    if not parallel_corpus:
        parallel_corpus = get_data.create_corpus(condensed_source, condensed_target)

    # Train fast_align model
    if not symmetrized_model:
        symmetrized_model = train_fa_model.train_model(parallel_corpus)

    # Get alignments
    print("Getting alignment scores...")
    df = get_best_alignment_scores(symmetrized_model, parallel_corpus, vrefs)

    # Remove duplicates
    avg_df = average_scores(df)

    # write results to csv
    if not outpath.exists():
        outpath.mkdir(parents=True)

    avg_df[avg_df.select_dtypes(['float']).columns] = avg_df.select_dtypes(['float']).astype('float16')
    avg_df = avg_df.drop(['alignment_score'], axis=1)
    avg_df.to_csv(outpath / "avg_alignment_scores.csv", index=False)

    df[df.select_dtypes(['float']).columns] = df.select_dtypes(['float']).astype('float16')
    df = df.drop(['alignment_count'], axis=1)
    df.to_csv(outpath / "alignment_scores.csv", index=False)

    # delete temp files
    condensed_source.unlink()
    condensed_target.unlink()

    return parallel_corpus, symmetrized_model


def main(args):
    sources = args.source_dir
    targets = args.target_dir
    base_outpath = args.outpath
    config_dir = args.config_dir

    for source_dir in sources.iterdir():
        print(source_dir)
        meta_file = source_dir / 'meta.json'
        with open(meta_file) as f:
            meta = json.load(f)
        source_str = meta['source']
        source = source_dir / f'{source_str}.txt'
        for target_dir in targets.iterdir():
            print(target_dir)
            meta_file = target_dir / 'meta.json'
            with open(meta_file) as f:
                meta = json.load(f)
            target_str = meta['source']
            config_file = config_dir / f'{target_str}-config.json'
            if config_file.exists():
                print("Found config file")
                with open(config_file) as f:
                    config = json.loads(f.read())
                requested_sources = config.get('sources', [])
                is_ref = config.get('ref', False)
                refresh = config.get('refresh', False)
                print(f'Is Ref? {is_ref}')
                print(f'Requested sources: {requested_sources}')
                if source_str not in requested_sources and not is_ref:
                    print(f"Skipping target {target_str} for source {source_str}")
                    continue
            target = target_dir / f'{target_str}.txt'
            outpath = base_outpath / f'{source_str}_{target_str}/'
            run_best_align(source, target, outpath, is_bible=args.is_bible)
            meta = {'source': source_str, 'target': target_str}
            with open(outpath / 'meta.json', 'w') as f:
                json.dump(meta, f)



if __name__ == "__main__":
    # command line args
    parser = argparse.ArgumentParser(description="Argparser")
    parser.add_argument("--source-dir", type=Path, help="source bible directory")
    parser.add_argument("--target-dir", type=Path, help="target bible directory")
    parser.add_argument("--outpath", type=Path, default=Path("/pfs/out"), help="Output directory")
    parser.add_argument("--config-dir", type=Path, help="Path to config dir", required=True)
    parser.add_argument("--is-bible", action="store_true", help="Whether text is Bible, in which case the length of the text file must be 41,899 lines")
    args, unknown = parser.parse_known_args()
    main(args)

