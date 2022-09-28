# Word Alignment

## fast_align.py
### Usage:

`python fast_align.py --source path/to/source/file --target path/to/target/file --threshold 0.5 --outpath /path/to/output/location`

**`--source` and `target` must be aligned `.txt` files!**
This script works for any two aligned text files; they do not have to be Bibles. 

### Output

A directory containing two files:

`in_context.csv`  Alignment pairs in the order they appear in `--source` and `target`. The word score threshold is not applied.
`sorted.csv`  Alignment pairs sorted alphabetically. Pairs are counted, duplicated are removed, word scores and verse scores are averaged, and the word score threshold is applied.

### Arguments:

`--source`  Source text file

`--target`  Target text file

`--threshold`  (default=`0.5`)  The fast_align word score threshold

`--outpath`  Output location for the resulting directory    

## match_words_in_aligned_verse.py
### Suggested usage:

`python match_words_in_aligned_verse.py --keys-name en-NLT --values-name greek --repo pabnlp`

### Output

A json file containing the word alignments. 

### Arguments:

`--keys-name`  The name of the first dataset. This dataset will provide the "keys" in the output dictionary.

`--values-name`  The name of the second dataset. This dataset will provide the "keys" in the output dictionary.

`--jaccard-similarity-threshold`  (default=`0.5`) The threshold for Jaccard Similarity for a match to be logged as significant and entered into the output dictionary (if it also meets the count-threshold).

`--count-threshold`  (default=`5`)  The threshold for count (number of occurences of the two items in the same verse) for a match to be logged as significant and entered into the output dictionary (if it also meets the jaccard-similarity-threshold).

`--logging-level`  (default=`info`)

`--refresh-cache`  Ignore any saved index caches or frequency caches.

`--outpath` Location where resulting files will be saved. 
