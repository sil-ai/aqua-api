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

#### Word by word all alignment scores:
These scores can be found in `summary_scores.csv`, and list every source-target word pair in each verse, with the following metrics:
* `verse_score`: The average of the Fast Align "average translation score" for the lines in which the pair co-occur.
* `alignment_score`: The average Fast Align "alignment score" for the pair, when Fast Align aligns them together.
* `co-occurrence_count`: The number of lines in which the pair appear together.
* `translation_score`: The "translation score" for the pair, from Fast Align. This score is a function of the two words, and does not depend on any particular context.
* `avg_aligned`: The number of times Fast Align aligns them together as a proportion of the number of lines they co-occur in. (This is normally <=1, but occasionally can be > 1 if they occur, and are aligned, multiple times in a single line.)
* `jac_sim`: The Jaccard Similarity of the lines where the source appears and the lines where the target appears.
* `match_counts`: The number of lines in which both the source and target word appear.
* `encoding_dist`: The euclidean distance between the embeddings of the two words in the Autoencoder.
* `simple_total`: The mean of `translation_score`, `alignment_score`, `avg_aligned` and `jac_sim`.
* `total_score`: The mean `translation_score`, `alignment_score`, `avg_aligned`, `jac_sim` and `encoding_dist`, which has been modified by x: log1p(1 - x) to apprximately map to [0, 1].

#### Word by word alignment best scores:
These scores can be found in `word_scores.csv`, which goes word by word through each verse, giving the best target match along with its scores. These files contain:
* `alignment_score`: The Fast Align "alignment score" for that pair in that verse.
* `translation_score`: The "translation score" for the pair, from Fast Align. This score is a function of the two words, and does not depend on any particular context.
* `avg_aligned`: The number of times Fast Align aligns them together as a proportion of the number of lines they co-occur in. (This is normally <=1, but occasionally can be > 1 if they occur, and are aligned, multiple times in a single line.)
* `jac_sim`: The Jaccard Similarity of the lines where the source appears and the lines where the target appears.
* `match_counts`: The number of lines in which both the source and target word appear.
* `encoding_dist`: The euclidean distance between the embeddings of the two words in the Autoencoder.
* `simple_total`: The mean of `translation_score`, `alignment_score`, `avg_aligned` and `jac_sim`.
* `total_score`: Experimental combining of the other metrics, which will almost certainly evolve. This could be the output of a model that takes the other features as inputs and predicts how good the alignment pair is.

#### Verse by verse alignment average scores
These scores can be found in `verse_scores.csv`, which goes verse by verse, listing the verse average of:
* `alignment_score`
* `translation_score`
* `avg_aligned`
* `jac_sim`
* `match_counts`
* `encoding_dist`
* `simple_total`
* `total_score`

### Arguments:

`--source`  Source text file

`--target`  Target text file

`--jaccard-similarity-threshold`  (default=`0.0`)  The Jaccard similarity threshold

`--count-threshold`  (default=`0`)  The threshold for count in match_words_in_aligned_verse (if it also meets the jaccard-similarity-threshold).

`--is-bible`  Boolean: if present, output will refer to lines by their verse references. Requires input text files to be 41,899 lines long.

`--outpath`  Output location for the resulting directory  


## red_flags.py
Compares the output from a source to target alignment with output from other alignments from the source to other reference targets, and highlights alignments in the original source-target matching that are significantly lower than the corresponding scores in the source-reference target alignments.

### Arguments:

`--source` The source text that alignments are coming from. Typically the original biblical language(s).

`--target` The target text that is being examined and compared with other reference targets.

`--reference` A list of other target texts, that `target` will be compared against. Ideally these are good quality, and ideally somewhat related to `target`.

`--outpath` The base output directory where the each of the data directories is located. If data for any particular alignment is not in this directory, the alignment will be run first.

`--refresh` Refresh the data - calculate the alignments and matches again, rather than using existing csv files.

`--refresh-cache` Refresh the index cache files.

`--combine-only` Only combine the results, since the alignment and matching files already exist.

`--exclude-encodings` Compute the scores using only the first four metrics, and not the autoencoder encodings.



### Outputs:

A file `red_flags.csv` file containing `vref`, `source` word, `total_score` from the best target word, and scores from the best target word from each reference translation. This list is filtered according to each `total_score < 0.1`, and each reference score `> 0.3`, with the average reference score being at least 5 times greater than `total_score` from the text in question.


## align.py
An implementation of [SIL Machine](https://github.com/sillsdev/machine.py/tree/main/machine)'s fast_align. Returns translation scores for all possible alignments in a line. Ignores word order, since the translation scores are calculated for two words over the whole corpus.
### Suggested Usage:

`python align.py --source path/to/source/file --target path/to/target/file --outpath /path/to/output/location`

### Output

A directory containing:

`translation_scores.csv`  All possible alignment pairs sorted alphabetically with their translation scores.

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

`alignment_scores_by_verse.csv`  Best alignment pairs in the order they appear in `--source` and `target`, with their alignment score in that particular context.

`avg_alignment_scores.csv`  Best alignment pairs sorted alphabetically. Pairs are grouped, and alignment scores and verse scores are averaged.

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

