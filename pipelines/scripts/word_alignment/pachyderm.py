from pathlib import Path
import argparse
import logging
from typing import Optional
import shutil

import combined


def run_pachyderm(
    source: Path,
    target: Path,
    outpath: Path,
    weights_path: Path=Path('data/models/encoder_weights.txt'),
    jaccard_similarity_threshold: float=0.05,
    count_threshold: int=0,
    is_bible: bool=True,
    refresh_cache: bool=True,
    ) -> None: 
                outpath = outpath / f"{source.stem}_{target.stem}"
                outpath.mkdir(parents=True, exist_ok=True)
                logging.info("Starting Fast Align")

                combined.run_fa(
                    source, 
                    target, 
                    outpath, 
                    is_bible=is_bible,
                    )
                logging.info("Starting Match Words")

                combined.run_match_words(
                                source, 
                                target, 
                                outpath,
                                jaccard_similarity_threshold=jaccard_similarity_threshold, 
                                count_threshold=count_threshold,
                                refresh_cache=refresh_cache,
                                )
                logging.info("Starting Combine Results")
                combined.run_combine_results(outpath)
                logging.info("Starting Combine By Verse Scores")
                combined.combine_by_verse_scores(
                            source, 
                            target, 
                            outpath, 
                            weights_path=weights_path, 
                            is_bible=is_bible,
                            )


def main(args):
    logging.basicConfig(
        format="%(asctime)s - %(funcName)20s() - %(message)s",
        level='INFO',
        filename=f"{args.outpath}/match_words_in_aligned_verse.log",
        filemode="a",
        force=True,
    )
    logging.info("START RUN")

    sources = args.source_dir
    targets = args.target_dir
    outpath = Path('/pfs/out/')
    # source_cache_dir = args.source_cache
    # target_cache_dir = args.target_cache
    for source in sources.iterdir():
        print(source)
        if source.suffix == 'json':
            shutil.copy(source, outpath / 'cache' / source.name)

    for target in targets.iterdir():
        print(target)
        if target.suffix == 'json':
            shutil.copy(target, outpath / 'cache' / target.name)

    for source in sources.iterdir():
        for target in targets.iterdir():
            print(f"Starting run")
            print(f"Source: {source}\nTarget: {target}")
            run_pachyderm(
            source = source,
            target = target,
            outpath = outpath,
            jaccard_similarity_threshold = args.jaccard_similarity_threshold,
            count_threshold = args.count_threshold,
            is_bible = args.is_bible,
            refresh_cache = args.refresh_cache,
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_dir", type=Path, help="source bible directory")
    parser.add_argument("--target_dir", type=Path, help="target bible directory")
    parser.add_argument("--outpath", type=Path, default=Path("/pfs/out"), help="Output directory")
    parser.add_argument("--source-cache", type=Path, help="Source cache directory")
    parser.add_argument("--target-cache", type=Path, help="Target cache directory")
    parser.add_argument("--jaccard-similarity-threshold", default=0.05, type=float, help="Jaccard Similarity threshold for including matches in dictionary")
    parser.add_argument("--count-threshold", type=int, default=0, help="Count threshold for including matches in dictionary")
    parser.add_argument("--is-bible", action="store_true", help="Whether text is Bible, in which case the length of the text file must be 41,899 lines")
    parser.add_argument("--refresh-cache", action="store_true", help="Refresh and overwrite the existing cache",)
    args = parser.parse_args()
    
    main(args)