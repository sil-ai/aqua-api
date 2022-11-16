from pathlib import Path
import argparse
import logging

import combined


def run_pachyderm(
    source: Path,
    target: Path,
    outpath: Path,
    jaccard_similarity_threshold: float=0.05,
    count_threshold: int=0,
    is_bible: bool=True,
    refresh_cache: bool=True,
    ) -> None: 
                outpath = outpath / f"{source.stem}_{target.stem}"
                outpath.mkdir(parents=True, exist_ok=True)
                combined.run_fa(
                    source, 
                    target, 
                    outpath, 
                    is_bible=is_bible,
                    )
                combined.run_match_words(
                                source, 
                                target, 
                                outpath, 
                                jaccard_similarity_threshold=jaccard_similarity_threshold, 
                                count_threshold=count_threshold,
                                refresh_cache=refresh_cache,
                                )
                combined.run_combine_results(outpath)


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
    for source in sources.iterdir():
        for target in targets.iterdir():
            run_pachyderm(
            source = source,
            target = target,
            outpath = Path('/pfs/out/'),
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
    parser.add_argument("--jaccard-similarity-threshold", default=0.05, type=float, help="Jaccard Similarity threshold for including matches in dictionary")
    parser.add_argument("--count-threshold", type=int, default=0, help="Count threshold for including matches in dictionary")
    parser.add_argument("--is-bible", action="store_true", help="Whether text is Bible, in which case the length of the text file must be 41,899 lines")
    parser.add_argument("--refresh-cache", action="store_true", help="Refresh and overwrite the existing cache",)
    args = parser.parse_args()
    
    main(args)