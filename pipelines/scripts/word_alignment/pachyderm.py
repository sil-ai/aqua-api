from pathlib import Path
import argparse
import logging
from typing import Optional
import shutil
import json

import combined
import get_data
import match


def run_pachyderm(
    source: Path,
    target: Path,
    outpath: Path,
    source_index_cache_file: Optional[Path]=None,
    target_index_cache_file: Optional[Path]=None,
    weights_path: Path=Path('data/models/encoder_weights.txt'),
    jaccard_similarity_threshold: float=0.05,
    count_threshold: int=0,
    is_bible: bool=True,
    refresh_cache: bool=True,
    ) -> None: 
                outpath = outpath / f"{source.stem}_{target.stem}"
                outpath.mkdir(parents=True, exist_ok=True)
                # logging.info("Starting Fast Align")

                combined.run_fa(
                    source, 
                    target, 
                    outpath, 
                    is_bible=is_bible,
                    )
                # logging.info("Starting Match Words")

                match.run_match(
                                source, 
                                target, 
                                outpath,
                                source_index_cache_file=source_index_cache_file,
                                target_index_cache_file=target_index_cache_file,
                                jaccard_similarity_threshold=jaccard_similarity_threshold, 
                                count_threshold=count_threshold,
                                refresh_cache=refresh_cache,
                                )
                # logging.info("Starting Combine Results")
                combined.run_combine_results(outpath)
                # logging.info("Starting Combine By Verse Scores")
                combined.combine_by_verse_scores(
                            source, 
                            target, 
                            outpath,
                            source_index_cache_file=source_index_cache_file,
                            target_index_cache_file=target_index_cache_file,
                            weights_path=weights_path, 
                            is_bible=is_bible,
                            )


def main(args):
    # logging.basicConfig(
    #     format="%(asctime)s - %(funcName)20s() - %(message)s",
    #     level='INFO',
    #     filename=f"{args.outpath}/{args.source_dir}_{args.target_dir}_match_words_in_aligned_verse.log",
    #     filemode="a",
    #     force=True,
    # )
    # logging.info("START RUN")

    sources = args.source_dir
    targets = args.target_dir
    base_outpath = Path('/pfs/out/')
    config_dir = args.config_dir

    for source_dir in sources.iterdir():
        print(source_dir)
        meta_file = source_dir / 'meta.json'
        with open(meta_file) as f:
            meta = json.load(f)
        source_str = meta['source']
        source_index_cache_file = source_dir / f'{source_str}-index-cache.json'
        source = source_dir / f'{source_str}.txt'
        for target_dir in targets.iterdir():
            print(target_dir)
            meta_file = target_dir / 'meta.json'
            with open(meta_file) as f:
                meta = json.load(f)
            config_file = config_dir / f'{source_str}-config.json'
            if config_file.exists():
                print("Found config file")
                with open(config_file) as f:
                    config = json.load(f)
                requested_sources = config['sources']
                print(f'Requested sources: {requested_sources}')
                if source_str not in requested_sources:
                    print(f"Skipping target {target_str} for source {source_str}")
                    continue
            target_str = meta['source']
            target_index_cache_file = target_dir / f'{target_str}-index-cache.json'
            target = target_dir / f'{target_str}.txt'
            print(f"Starting run")
            print(f"Source: {source}\nTarget: {target}")
            outpath = base_outpath / f'{source_str}_{target_str}/'
            run_pachyderm(
            source = source,
            target = target,
            outpath = base_outpath,
            source_index_cache_file=source_index_cache_file,
            target_index_cache_file=target_index_cache_file,
            jaccard_similarity_threshold = args.jaccard_similarity_threshold,
            count_threshold = args.count_threshold,
            is_bible = args.is_bible,
            refresh_cache = args.refresh_cache,
            )
            meta = {
                'source': source.stem,
                'target': target.stem,
            }
            get_data.write_dictionary_to_file(meta, outpath / 'meta.json')
            source_file_path = outpath / source.name
            target_file_path = outpath / target.name
            source_index_cache_file_path = outpath / source_index_cache_file.name
            target_index_cache_file_path = outpath / target_index_cache_file.name
            if not source_file_path.exists():
                shutil.copy(source, source_file_path)
            if not target_file_path.exists():
                shutil.copy(target, outpath / target.name)
            if not source_index_cache_file_path.exists():
                shutil.copy(source_index_cache_file, source_index_cache_file_path)
            if not target_index_cache_file_path.exists():
                shutil.copy(target_index_cache_file, target_index_cache_file_path)
            if (base_outpath / 'cache').exists():
                shutil.rmtree(base_outpath / 'cache')



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_dir", type=Path, help="source bible directory")
    parser.add_argument("--target_dir", type=Path, help="target bible directory")
    parser.add_argument("--outpath", type=Path, default=Path("/pfs/out"), help="Output directory")
    parser.add_argument("--config-dir", type=Path, help="Path to config dir", required=True)
    parser.add_argument("--jaccard-similarity-threshold", default=0.05, type=float, help="Jaccard Similarity threshold for including matches in dictionary")
    parser.add_argument("--count-threshold", type=int, default=0, help="Count threshold for including matches in dictionary")
    parser.add_argument("--is-bible", action="store_true", help="Whether text is Bible, in which case the length of the text file must be 41,899 lines")
    parser.add_argument("--refresh-cache", action="store_true", help="Refresh and overwrite the existing cache",)
    args = parser.parse_args()
    
    main(args)