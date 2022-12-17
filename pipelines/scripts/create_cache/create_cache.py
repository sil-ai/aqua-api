import argparse
from pathlib import Path
import shutil

import get_data


def create_index_cache(source, outpath, is_bible:bool=True):
    cache_path = outpath
    index_cache_file = cache_path / f'{source.stem}-index-cache.json'
    get_data.create_words(source, index_cache_file, outpath, is_bible=is_bible)
    return index_cache_file


def create_meta_file(source, outpath):
    meta = {
            'source': source.stem,
        }
    meta_file = outpath / 'meta.json'
    get_data.write_dictionary_to_file(meta, meta_file)
    return meta_file


def main(args):
    if args.source_dir.exists():  # Note that in this file, "source" can be either a source or a target.
        for source in args.source_dir.iterdir():
            outpath = Path(f'/pfs/out/{source.stem}')
            create_index_cache(source, outpath, is_bible=True)
            create_meta_file(source, outpath)
            shutil.copy(source, outpath / source.name)


if __name__ == '__main__':
     
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", type=Path, help="Path to source dir", required=True)
    args = parser.parse_args()
    main(args)
