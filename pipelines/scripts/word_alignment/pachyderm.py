from pathlib import Path
import logging
from combined import run_fa, run_match_words, run_combine_results
import os
import argparse


def run_pachyderm(
    data_dir: Path,
    ref_dir: Path,
    outpath: Path,
    jaccard_similarity_threshold: float=0.0,
    count_threshold: int=0,
    is_bible: bool=True,
    refresh_cache: bool=True,
    ) -> None: 
    for file in data_dir.iterdir():
        print(f"File 1: {file}")
        file = Path(file)
        for ref_file in ref_dir.iterdir():
            if ref_file.stem != 'vref':
                print(f"File 2: {ref_file}")
                ref_file = Path(ref_file)
                outpath = outpath / f"{file.stem}_{ref_file.stem}"
                if file != ref_file:
                    run_fa(
                        file, 
                        ref_file, 
                        outpath, 
                        is_bible=is_bible,
                        )
                    run_match_words(
                                    file, 
                                    ref_file, 
                                    outpath, 
                                    jaccard_similarity_threshold=jaccard_similarity_threshold, 
                                    count_threshold=count_threshold,
                                    refresh_cache=refresh_cache,
                                    )
                run_combine_results(outpath)


def main(args):
    run_pachyderm(
    data_dir = args.data_dir,
    ref_dir = args.ref_dir,
    outpath = args.outpath,
    jaccard_similarity_threshold = args.jaccard_similarity_threshold,
    count_threshold = args.count_threshold,
    is_bible = args.is_bible,
    refresh_cache = args.refresh_cache,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("/pfs/text_data"), help="Directory for input (source) files")
    parser.add_argument("--ref-dir", type=Path, default=Path('/pfs/ref_data'), help="Directory for reference (target) files")
    parser.add_argument("--outpath", type=Path, default=Path("/pfs/out"), help="Output directory")
    parser.add_argument("--jaccard-similarity-threshold", default=0.0, type=float, help="Jaccard Similarity threshold for including matches in dictionary")
    parser.add_argument("--count-threshold", type=int, default=0, help="Count threshold for including matches in dictionary")
    parser.add_argument("--is-bible", action="store_true", help="Whether text is Bible, in which case the length of the text file must be 41,899 lines")
    parser.add_argument("--refresh-cache", action="store_true", help="Refresh and overwrite the existing cache",)
    args = parser.parse_args()
    main(args)