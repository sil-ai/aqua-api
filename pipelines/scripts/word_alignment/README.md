# Word Alignment

Word alignment scripts. These scripts work for any two aligned text files; they do not have to be Bibles.

**`--source` and `--target` must be aligned `.txt` files!** 

## align.py
An implementation of [SIL Machine](https://github.com/sillsdev/machine.py/tree/main/machine)'s fast_align
### Suggested Usage:

`python align.py --source path/to/source/file --target path/to/target/file --threshold 0.5 --outpath /path/to/output/location`

### Output

A directory containing two files:

`in_context.csv`  Alignment pairs in the order they appear in `--source` and `target`. The word score threshold is not applied.
`sorted.csv`  Alignment pairs sorted alphabetically. Pairs are counted, duplicated are removed, word scores and verse scores are averaged, and the word score threshold is applied.

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
`python combined.py --source path/to/source/file --target path/to/target/file --is-bible True --outpath /path/to/output/location`

### Output:
A directory containing:

1) a directory with fast_align data
2) a directory with match_words_in_aligned_verse data
3) a csv with the combined data from both algorithms. 

*(Note: If an alignment is predicted by one algorithm but not the other, this will be represented by -1 in the combined dataframe.)*

### Arguments:

`--source`  Source text file

`--target`  Target text file

`--word-score-threshold`  (default=`0.5`)  The fast_align word score threshold

`--jaccard-similarity-threshold`  (default=`0.5`)  The Jaccard similarity threshold

`--is-bible`  (default=`False`)  If `True`, will refer to lines by their verse references in the fast_align output files

`--count-threshold`  (default=`1`)  The threshold for count in match_words_in_aligned_verse (if it also meets the jaccard-similarity-threshold).

`--outpath`  Output location for the resulting directory  

