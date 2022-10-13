# Word Alignment

Word alignment scripts. These scripts work for any two aligned text files; they do not have to be Bibles.

**`--source` and `--target` must be aligned `.txt` files!** 

## align.py
An implementation of [SIL Machine](https://github.com/sillsdev/machine.py/tree/main/machine)'s fast_align. Returns translation scores for all possible alignments in a line. Ignores word order, since the translation scores are calculated for two words over the whole corpus.
### Suggested Usage:

`python align.py --source path/to/source/file --target path/to/target/file --outpath /path/to/output/location`

### Output

A directory containing two files:

`all_in_context.csv`  All possible alignment pairs in the order they appear in `--source` and `target`.

`all_sorted.csv`  All possible alignment pairs sorted alphabetically with their translation scores.

### Arguments:

`--source`  Source text file

`--target`  Target text file

`--outpath`  Output location for the resulting directory   

`--is-bible`  Boolean: if present, output will refer to lines by their verse references. Requires input text files to be 41,899 lines long.

## align_best.py
An implementation of [SIL Machine](https://github.com/sillsdev/machine.py/tree/main/machine)'s fast_align. Returns only the best alignment for each word in each line. Takes word order into account. 
### Suggested Usage:

`python align_best.py --source path/to/source/file --target path/to/target/file --outpath /path/to/output/location`

### Output

A directory containing two directories (one for each alignment direction), each containing three files:

`best_in_context.csv`  Best alignment pairs in the order they appear in `--source` and `target`, with their alignment score in that particular context.

`best_sorted.csv`  Best alignment pairs sorted alphabetically. Pairs are grouped, and alignment scores and verse scores are averaged.

`best_vref_scores.csv` Average alignment scores for each line (or verse, if `--is-bible`)

### Arguments:

`--source`  Source text file

`--target`  Target text file

`--outpath`  Output location for the resulting directory   

`--is-bible`  Boolean: if present, output will refer to lines by their verse references. Requires input text files to be 41,899 lines long.

## match.py
A modified version of Mark Woodward's [match_words_in_aligned_verse](https://github.com/sil-ai/new2old) algorithm
### Suggested Usage:

`python match.py --keys-name path/to/source/file --values-name path/to/target/file --jaccard-similarity-threshold 0.5 --outpath /path/to/output/location`

### Output

A directory containing cache, a log, and a `dictionary.json` file with the word alignments. 

### Arguments:

`--keys-name`  The name of the first dataset. This dataset will provide the "keys" in the output dictionary.

`--values-name`  The name of the second dataset. This dataset will provide the "keys" in the output dictionary.

`--jaccard-similarity-threshold`  (default=`0.0`) The threshold for Jaccard Similarity for a match to be logged as significant and entered into the output dictionary (if it also meets the count-threshold).

`--count-threshold`  (default=`0`)  The threshold for count (number of occurences of the two items in the same verse) for a match to be logged as significant and entered into the output dictionary (if it also meets the jaccard-similarity-threshold).

`--logging-level`  (default=`INFO`)

`--refresh-cache`  Ignore any saved index caches or frequency caches.

`--outpath` Location where resulting files will be saved. 

## combined.py
Combines `align.py` and `match.py`. Takes about 20 minutes to align two bible texts. 
vrefs are from [here](https://github.com/sil-ai/aqua-api/tree/master/fixtures)
### Suggested Usage:
`python combined.py --source path/to/source/file --target path/to/target/file --is-bible --outpath /path/to/output/location`

### Output:
A directory named `<source>_<target>` containing various files (mainly for debugging). The main output file is:

* combined.csv

which lists all source-target combinations, with three metrics:
    
* `translation_score`:     The translation score the Fast Align trained model gives for these two words
* `alignment_score`:       The average alignment score from Fast Align when these two words appeared together.
* `jac_sim`:               The Jaccard Similarity of the lines where the source appears and the lines where the target appears.

These three metrics are somewhat independent of each other, and can be combined to give a score as to how well the two words correlate with each other. They could be combined in different ways to give an overall score, depending on the downstream task. The various scores in this spreadsheet could even be used as inputs to train a model to predict alignments.

### Arguments:

`--source`  Source text file

`--target`  Target text file

`--jaccard-similarity-threshold`  (default=`0.0`)  The Jaccard similarity threshold

`--count-threshold`  (default=`0`)  The threshold for count in match_words_in_aligned_verse (if it also meets the jaccard-similarity-threshold).

`--is-bible`  Boolean: if present, output will refer to lines by their verse references. Requires input text files to be 41,899 lines long.

`--outpath`  Output location for the resulting directory  

