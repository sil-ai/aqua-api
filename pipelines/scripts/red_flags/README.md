# Red Flags

This pipeline takes the [Top Source Scores](../top_source_scores/) for each verse - source word combination and filters them so that only those `< threshold(=0.1)` are retained. These are outputed as `possible_red_flags.csv`.

These low scores are then compared with the mean scores from the same source compared with other target texts (from [Create Refs](../create_refs/)). Those where the mean is `> 0.35` and the mean is at least five times the score from the target in question, are recorded in `red_flags.csv`.

Note that if there are no other target texts that have been compared with this source, then `possible_red_flags.csv` and `red_flags.csv` should be identical.