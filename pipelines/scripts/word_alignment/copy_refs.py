from pathlib import Path
import argparse
import shutil


def main(args):
    shutil.copytree(args.inpath, args.outpath)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--inpath", type=Path, help="Path to base directory where scores are saved", required=True)
    parser.add_argument("--outpath", type=Path, help="Path to base outpath directory", required=True)

    args = parser.parse_args()
    main(args)
