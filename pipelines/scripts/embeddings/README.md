# Embeddings

This pipeline calculates the 50-dimensional embedding vector for each word, derived from multiplying the [encoder weights](./weights/encoder_weights.txt) by a 41,899-dimenstional vector corresponding to a one-hot encoding of the verses in which the word occurs.

For each pair of words throughout the corpus, the embedding distance is then calculated, defined as the `np.linalg.norm` distance between the two encodings. This is recorded in the `embeddings.csv` file.