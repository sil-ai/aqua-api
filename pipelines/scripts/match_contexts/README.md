# Match Contexts

This pipeline takes a source and a target (together with their index cache files) and the target config file as input. If the target config file requests that the target be compared with a particular source, the pipeline takes each source and target word and counts the number of times they occur together in a verse, compared with the number of times one of them occurs without the other.

The [Jaccard Similarity score](https://en.wikipedia.org/wiki/Jaccard_index) for each pair is recorded in `dictionary.json`, together with a count of how many times they occur together.