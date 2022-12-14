# Total Scores

This pipeline takes the outputs from the following four pipelines:
* [Alignment Scores](../alignment_scores/)
* [Translation Scores](../translation_scores/)
* [Match Contexts](../match_contexts/)
* [Embeddings](../embeddings/)

and creates a single `total_score` for each verse - source word - target word combination in the corpus. This total score is just a simple mean of the main four metrics, plus a fifth metric which is the average number of times Fast Align aligns the words when they appear in the same verse.

These scores are outputed to `total_scores.csv`.
