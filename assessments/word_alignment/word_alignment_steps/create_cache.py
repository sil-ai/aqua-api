import argparse
from pathlib import Path
import shutil
import os
import sys

from typing import Dict

sys.path.append((Path(__file__).parent.parent).as_posix())

import common_files.get_data as get_data


def create_index_cache(source, outpath, is_bible:bool=True):
    index_cache = {}
    print(source.absolute())
    ref_df = get_data.get_ref_df(source, is_bible=is_bible)
    ref_df = get_data.get_words_from_txt_file(ref_df, outpath)
    index_cache = get_data.create_words_from_df(ref_df)
    return index_cache


def create_meta_file(source, outpath):
    meta = {
            'source': source.stem,
        }
    meta_file = outpath / 'meta.json'
    get_data.write_dictionary_to_file(meta, meta_file)
    return meta_file



def main(args):
    # if args.source_dir.exists():  # Note that in this file, "source" can be either a source or a target.
    #     for source in args.source_dir.iterdir():
    source = args.source
    outpath = args.outpath
    cache_path = outpath / 'cache'
    index_cache_file = cache_path / f'{source.stem}-index-cache.json'
    word_dict_lang = create_index_cache(source, outpath, is_bible=True)
    index_cache = {word.word: word.index_list for word in word_dict_lang.values()}
    # create_meta_file(source, outpath)
    get_data.save_word_dict_lang_to_cache(index_cache, index_cache_file)
    
    # shutil.copy(source, outpath / source.name)


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", type=Path, help="Path to source dir", required=True)
    parser.add_argument("--outpath", type=Path, help="Path to output dir", required=True)
    args = parser.parse_args()
    main(args)
