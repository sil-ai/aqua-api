from pathlib import Path
import logging
from combined import run_fa, run_match_words, combine_df
import os

def main():
    jaccard_similarity_threshold = 0.01
    count_threshold = 0
    is_bible = True
    refresh_cache = False
    outpath = Path('pfs/out')
    ref_dir = Path('pfs/ref_data')
    for dirpath, dirs, files in os.walk("pfs/text_data"):
        for file in files:
            print(f"File 1: {file}")

            file = Path(file)
            for ref_dirpath, _, ref_files in os.walk(ref_dir):
                for ref_file in ref_files:
                    if ref_file != 'vref.txt':
                        print(f"File 2: {ref_file}")

                        ref_file = Path(ref_file)
                        if file != ref_file:
                            out_file = outpath / f'{file.stem}_{ref_file.stem}_match' / f'{file.stem}-{ref_file.stem}_ref_df.csv'
                            print(out_file)
                            if not out_file.exists():
                                run_fa(
                                    Path(dirpath) / file, 
                                    Path(ref_dirpath) / ref_file, 
                                    outpath, 
                                    is_bible=is_bible,
                                    )
                            run_match_words(
                                            Path(dirpath) / file, 
                                            Path(ref_dirpath) / ref_file, 
                                            outpath, 
                                            jaccard_similarity_threshold=jaccard_similarity_threshold, 
                                            count_threshold=count_threshold,
                                            refresh_cache=refresh_cache,
                                            )
                        df = combine_df(outpath, file.stem, ref_file.stem)
                        # reverse_df = combine_df(outpath, ref_file.stem, file.stem)

                        #save results
                        path = outpath / f'{file.stem}_{ref_file.stem}_combined'
                        if not path.exists():
                            path.mkdir(exist_ok=True)
                        # reverse_path = outpath / f'{ref_file.stem}_{file.stem}_combined'
                        # if not reverse_path.exists():
                        #     reverse_path.mkdir(exist_ok=True)
                        df.to_csv(path / f'{file.stem}_{ref_file.stem}_combined.csv')
                        # reverse_df.to_csv(reverse_path / f'{ref_file.stem}_{file.stem}_combined.csv')


if __name__ == "__main__":
    main()