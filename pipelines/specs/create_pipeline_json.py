import argparse
import json
from pathlib import Path

def main(args):
    tag = args.tag
    imagename = args.imagename
    file = args.file
    template_file = file.parent / f"{file.stem}_template{file.suffix}"
    with open(template_file) as f:
        data = json.load(f)

    data['transform']['image'] = data['transform']['image'].replace('<imagename>', imagename).replace('<tag>', tag)

    with open(file, 'w') as f:
        json.dump(data, f, indent=4)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=Path, help="Path to json file to write", required=True)
    parser.add_argument("--tag", type=str, help="String to use for tag", required=True)
    parser.add_argument("--imagename", type=str, help="Image name to use for image", required=True)

    args = parser.parse_args()
    main(args)
