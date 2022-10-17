# Word Alignment

Word alignment scripts. These scripts work for any two aligned text files; they do not have to be Bibles.

**`--source` and `--target` must be aligned `.txt` files!** 

## General usage: combined.py
Combines `align.py`, `align_best.py` and `match.py`. Takes about 20 minutes to align two bible texts. 
vrefs are from [here](https://github.com/sil-ai/aqua-api/tree/master/fixtures)

### Suggested Usage:
`python combined.py --source path/to/source/file --target path/to/target/file --is-bible --outpath /path/to/output/location`

### Output
All output is placed in a directory named `<source>_<target>` containing various files. The main outputs are:

#### Word by word lexical scores:
These scores can be found in `combined.csv`, and list every source-target word pair in the corpus, with the following metrics:
* `co-occurrence_count`: The number of lines in which the pair appear together.
* `translation_score`: The "translation score" for the pair, from Fast Align. This score is a function of the two words, and does not depend on any particular context.
* `verse_score`: The average of the Fast Align "average translation score" for the lines in which the pair co-occur.
* `alignment_score`: The average Fast Align "alignment score" for the pair, when Fast Align aligns them together.
* `avg_aligned`: The number of times Fast Align aligns them together as a proportion of the number of lines they co-occur in. (This is normally <=1, but occasionally can be > 1 if they occur, and are aligned, multiple times in a single line.)
* `normalized_source`: The "normalized" source word, with punctuation, diacritics, etc removed.
* `normalized_target`: The "normalized" target word, with punctuation, diacritics, etc removed.
* `jac_sim`: The Jaccard Similarity of the lines where the source appears and the lines where the target appears.
* `match_counts`: The number of lines in which both the source and target word appear.

#### Word by word alignment scores:
These scores can be found in `best_in_context.csv`, which goes line by line, listing the Fat Align alignment pairs. Note that `hebrew_key_terms.csv` and `greek_key_terms.csv` contain the same information, filtered by "major key terms", as defined in the [SILNLP repository](https://github.com/sillsdev/silnlp/tree/master/silnlp/assets). These files contain:
* `FA_verse_score`: The Fast Align "average translation score" for that verse. This is the mean of `translation_score` for the aligned pairs in that verse.
* `FA_alignment_score`: The Fast Align "alignment score" for that pair in that verse.
* `co-occurrence_count`: The number of lines in which the pair appear together.
* `translation_score`: The "translation score" for the pair, from Fast Align. This score is a function of the two words, and does not depend on any particular context.
* `avg_FA_alignment_score`: The average Fast Align alignment score for this word pair throughout the corpus, when Fast Align aligns them together.
* `avg_aligned`: The number of times Fast Align aligns them together as a proportion of the number of lines they co-occur in. (This is normally <=1, but occasionally can be > 1 if they occur, and are aligned, multiple times in a single line.)
* `jac_sim`: The Jaccard Similarity of the lines where the source appears and the lines where the target appears.
* `match_counts`: The number of lines in which both the source and target word appear.
* `total_score`: Experimental combining of the other metrics, which will almost certainly evolve. This could be the output of a model that takes the other features as inputs and predicts how good the alignment pair is.

#### Verse by verse alignment
These scores can be found in `verse_scores.csv`, which goes verse by verse, listing:
* `FA_verse_score`: The average of the Fast Align "average translation score" for the lines in which the pair co-occur.
* `avg_aligned`: The number of times Fast Align aligns a pair together as a proportion of the number of lines they co-occur in, averaged over all aligned pairs in the line.
* `avg_FA_alignment_score`: The average Fast Align alignment score for the pairs in the line.
* `jac_sim`: The average Jaccard Similarity of the lines where the source appears and the lines where the target appears, averaged across the aligned pairs in the line.
* `total_score`: The average of the experimental `total_score` metric for each aligned word pair in the line.

### Arguments:

`--source`  Source text file

`--target`  Target text file

`--jaccard-similarity-threshold`  (default=`0.0`)  The Jaccard similarity threshold

`--count-threshold`  (default=`0`)  The threshold for count in match_words_in_aligned_verse (if it also meets the jaccard-similarity-threshold).

`--is-bible`  Boolean: if present, output will refer to lines by their verse references. Requires input text files to be 41,899 lines long.

`--outpath`  Output location for the resulting directory  



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

`python match.py --source path/to/source/file --target path/to/target/file --jaccard-similarity-threshold 0.5 --outpath /path/to/output/location`

### Output

A directory containing cache, a log, and a `dictionary.json` file with the word alignments. 

### Arguments:

`--source`  The path to the source txt file.

`--target`  The path to the target txt file.

`--jaccard-similarity-threshold`  (default=`0.0`) The threshold for Jaccard Similarity for a match to be logged as significant and entered into the output dictionary (if it also meets the count-threshold).

`--count-threshold`  (default=`0`)  The threshold for count (number of occurences of the two items in the same verse) for a match to be logged as significant and entered into the output dictionary (if it also meets the jaccard-similarity-threshold).

`--logging-level`  (default=`INFO`)

`--refresh-cache`  Ignore any saved index caches or frequency caches.

`--outpath` Location where resulting files will be saved. 

