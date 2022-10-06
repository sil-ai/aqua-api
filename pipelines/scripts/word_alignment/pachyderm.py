from pathlib import Path
import logging
from combined import run_fa, run_match_words, combine_df
import os

def main():
    word_score_threshold = 0.01
    jaccard_similarity_threshold = 0.01
    count_threshold = 0
    outpath = Path('pfs/out')
    ref_dir = Path('pfs/ref_data')
    for dirpath, dirs, files in os.walk("pfs/text_data"):
        for file in files:
            print(f"File 1: {file}")

            file = Path(file)
            for ref_dirpath, _, ref_files in os.walk(ref_dir):
                for ref_file in ref_files:
                    print(f"File 2: {ref_file}")

                    ref_file = Path(ref_file)
                    if file != ref_file:
                        out_file = outpath / f'{file.stem}_{ref_file.stem}_match' / f'{file.stem}-{ref_file.stem}_ref_df.csv'
                        print(out_file)
                        if not out_file.exists():
                            run_fa(
                                Path(os.path.join(dirpath, file)), 
                                Path(os.path.join(ref_dirpath, ref_file)), 
                                word_score_threshold, 
                                Path(outpath), 
                                'True'
                                )
                        run_match_words(
                                        Path(os.path.join(dirpath, file)), 
                                        Path(os.path.join(ref_dirpath, ref_file)), 
                                        Path(outpath), 
                                        jaccard_similarity_threshold=jaccard_similarity_threshold, 
                                        count_threshold=count_threshold,
                                        refresh_cache=False
                                        )
                    df = combine_df(outpath, file.stem, ref_file.stem)
                    #save results
                    print(df['jac_sim'])
                    df.to_csv(outpath / f'{file.stem}_combined.csv')

if __name__ == "__main__":
    main()