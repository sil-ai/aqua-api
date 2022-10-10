# Word Alignment

Word alignment scripts. These scripts work for any two aligned text files; they do not have to be Bibles.

**`--source` and `--target` must be aligned `.txt` files!** 

## align.py
An implementation of [SIL Machine](https://github.com/sillsdev/machine.py/tree/main/machine)'s fast_align. Returns alignment scores for all possible alignments in a line. Ignores word order. 
### Suggested Usage:

`python align.py --source path/to/source/file --target path/to/target/file --threshold 0.5 --outpath /path/to/output/location`

### Output

A directory containing two files:

`in_context.csv`  All possible alignment pairs in the order they appear in `--source` and `target`. The word score threshold is not applied.

`sorted.csv`  All possible alignment pairs sorted alphabetically. Pairs are counted, duplicated are removed, word scores are averaged, and the word score threshold is applied.

### Arguments:

`--source`  Source text file

`--target`  Target text file

`--threshold`  (default=`0.5`)  The fast_align word score threshold

`--outpath`  Output location for the resulting directory   

`--is-bible`  (default=`False`)  If `True`, will refer to lines by their verse references in the fast_align output files

## align_best.py
An implementation of [SIL Machine](https://github.com/sillsdev/machine.py/tree/main/machine)'s fast_align. Returns only the best alignment for each word in each line. Takes word order into account. 
### Suggested Usage:

`python align_best.py --source path/to/source/file --target path/to/target/file --threshold 0.5 --outpath /path/to/output/location`

### Output

A directory containing two directories (one for each alignment direction), each containing three files:

`in_context.csv`  Best alignment pairs in the order they appear in `--source` and `target`. The word score threshold is not applied.

`sorted.csv`  Best alignment pairs sorted alphabetically. Pairs are counted, duplicated are removed, word and verse scores are averaged, and the word score threshold is applied.

`vref_scores.csv` Average alignment scores for each line (or verse, if `--is-bible` is `True`)

### Arguments:

`--source`  Source text file

`--target`  Target text file

`--threshold`  (default=`0.5`)  The fast_align word score threshold

`--outpath`  Output location for the resulting directory   

`--is-bible`  (default=`False`)  If `True`, will refer to lines by their verse references in the fast_align output files

## match.py
A modified version of Mark Woodward's [match_words_in_aligned_verse](https://github.com/sil-ai/new2old) algorithm
### Suggested Usage:

`python match.py --keys-name path/to/source/file --values-name path/to/target/file --jaccard-similarity-threshold 0.5 --outpath /path/to/output/location`

### Output

A directory containing cache, a log, and a json file with the word alignments. 

### Arguments:

`--keys-name`  The name of the first dataset. This dataset will provide the "keys" in the output dictionary.

`--values-name`  The name of the second dataset. This dataset will provide the "keys" in the output dictionary.

`--jaccard-similarity-threshold`  (default=`0.5`) The threshold for Jaccard Similarity for a match to be logged as significant and entered into the output dictionary (if it also meets the count-threshold).

`--count-threshold`  (default=`1`)  The threshold for count (number of occurences of the two items in the same verse) for a match to be logged as significant and entered into the output dictionary (if it also meets the jaccard-similarity-threshold).

`--logging-level`  (default=`info`)

`--refresh-cache`  Ignore any saved index caches or frequency caches.

`--outpath` Location where resulting files will be saved. 

## combined.py
Combines `align.py` and `match.py`. Takes about 20 minutes to align two bible texts. 
vrefs are from [here](https://github.com/sil-ai/aqua-api/tree/master/fixtures)
### Suggested Usage:
`python combined.py --source path/to/source/file --target path/to/target/file --is-bible --outpath /path/to/output/location`

### Output:
A directory containing:

1) a directory with fast_align data (in `_align` directory)
2) a directory with fast_align "best" data (in `_align_best` directory)
2) a directory with match_words_in_aligned_verse data (in `_match` directory)
3) a csv with the combined data from both algorithms (in `_combined` directory)

Note that the main output is the `_combined.csv` file in the `_combined` directory. This file lists all source-target combinations, with three metrics:
    
* `FA_translation_score`:     The translation score the Fast Align trained model gives for these two words
* `avg_aligned`:              The proportion of times the Fast Align trained model aligned these two words when they appeared together.
* `jac_sim`:                  The Jaccard Similarity of the lines where the source appears and the lines where the target appears.

These three metrics are somewhat independent of each other, and can be combined to give a score as to how well the two words correlate with each other.

### Arguments:

`--source`  Source text file

`--target`  Target text file

`--align-best-only`  (default=`False`) Runs `align_best.py` if `True`, `align.py` otherwise

`--word-score-threshold`  (default=`0.5`)  The fast_align word score threshold

`--jaccard-similarity-threshold`  (default=`0.5`)  The Jaccard similarity threshold

`--is-bible`  If present, output will refer to lines by their verse references

`--count-threshold`  (default=`1`)  The threshold for count in match_words_in_aligned_verse (if it also meets the jaccard-similarity-threshold).

`--outpath`  Output location for the resulting directory  

