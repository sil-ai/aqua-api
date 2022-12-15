# Create Refs

This pipeline takes the output from the [Top Source Scores](../top_source_scores/) pipeline and aggregates them into a single dataframe for each source text. The various target texts that have been aligned with that source appear as columns, and the `total_score` for each verse - source word combination for a particular target text appear in its column.

These scores are outputed to `summary_top_source_scores.csv`.
