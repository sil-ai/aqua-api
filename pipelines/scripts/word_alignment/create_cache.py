import argparse
from pathlib import Path
import shutil

import get_data

def main(args):
    if args.source_dir.exists():
        for source in args.source_dir.iterdir():
            outpath = Path(f'/pfs/out/{source.stem}')
            cache_path = outpath
            get_data.create_words(source, cache_path, outpath)
            shutil.copy(source, outpath / source.name)


if __name__ == '__main__':
     
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", type=Path, help="Path to source text", required=True)
    args = parser.parse_args()
    main(args)