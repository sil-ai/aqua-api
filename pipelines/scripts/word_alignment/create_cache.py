import argparse
from pathlib import Path
import shutil

import get_data

def main(args):
    if args.source_dir.exists():
        for source in args.source_dir.iterdir():
            outpath = Path(f'/pfs/out/{source.stem}')
            cache_path = outpath
            index_cache_file = cache_path / f'{source.stem}-index-cache.json'
            get_data.create_words(source, index_cache_file, outpath)
            (outpath / 'src.txt').unlink()
            meta = {
                    'source': source.stem,
                }
            get_data.write_dictionary_to_file(meta, outpath / 'meta.json')
            shutil.copy(source, outpath / source.name)


if __name__ == '__main__':
     
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", type=Path, help="Path to source text", required=True)
    args = parser.parse_args()
    main(args)