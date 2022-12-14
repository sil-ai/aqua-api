from typing import Optional
from pathlib import Path
import argparse
import json

import pandas as pd
import numpy as np

import get_data

def get_embeddings(
        source,
        target,
        outpath,
        source_index_cache_file: Optional[Path]=None,
        target_index_cache_file: Optional[Path]=None,
        is_bible: bool=True,
        weights_path: Path=Path('data/models/encoder_weights.txt'),
        ):
    word_dict_src = get_data.create_words(source, source_index_cache_file, outpath, is_bible=is_bible)
    word_dict_trg = get_data.create_words(target, target_index_cache_file, outpath, is_bible=is_bible)

    weights = np.loadtxt(weights_path)
    for word in ({**word_dict_src, **word_dict_trg}.values()):
        word.get_encoding(weights)

    ref_df = get_data.get_ref_df(source, target, is_bible=is_bible)
    ref_df = get_data.condense_files(ref_df)
    ref_df = get_data.get_words_from_txt_file(ref_df, outpath)
    df = ref_df.explode('src_words').explode('trg_words')
    df.loc[:, 'embedding_dist'] = df.apply(lambda row: word_dict_src[row['src_words']].get_norm_distance(word_dict_trg[row['trg_words']]))
    df.to_csv(outpath / "embeddings.csv", index=False)


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
                # refresh = config.get('refresh', False)
                print(f'Is Ref? {is_ref}')
                print(f'Requested sources: {requested_sources}')
                if source_str not in requested_sources and not is_ref:
                    print(f"Skipping target {target_str} for source {source_str}")
                    continue
            target = target_dir / f'{target_str}.txt'
            outpath = base_outpath / f'{source_str}_{target_str}/'
            source_index_cache_file = source_dir / f'{source_str}-index-cache.json'
            target_index_cache_file = target_dir / f'{target_str}-index-cache.json'

            get_embeddings(
                source,
                target,
                outpath,
                source_index_cache_file=source_index_cache_file,
                target_index_cache_file=target_index_cache_file,
                is_bible=args.is_bible,
                weights_path=args.weights_path
            )


if __name__ == "__main__":
    pd.options.mode.chained_assignment = None  # default='warn'
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", type=Path, help="source bible directory")
    parser.add_argument("--target-dir", type=Path, help="target bible directory")
    parser.add_argument("--outpath", type=Path, default=Path("/pfs/out"), help="Output directory")
    parser.add_argument("--config-dir", type=Path, help="Path to config dir", required=True)
    parser.add_argument("--weights-path", type=Path, help="Path to embedding weights file")
    parser.add_argument("--is-bible", action="store_true", help="Whether text is Bible, in which case the length of the text file must be 41,899 lines")
    parser.add_argument("--jaccard-similarity-threshold", type=float, help="Threshold for Jaccard Similarity score to be significant", default=0.05)
    parser.add_argument("--count-threshold", type=int, help="Threshold for count (number of co-occurences) score to be significant", default=0)
    parser.add_argument("--refresh-cache", action="store_true", help="Refresh and overwrite the existing cache")
    args = parser.parse_args()

    main(args)