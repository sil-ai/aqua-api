import os
from pathlib import Path

import modal

word_alignment_results_volume = modal.NetworkFileSystem.from_name(
    "word_alignment_results", create_if_missing=True
)


# Manage suffix on modal endpoint if testing.
suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix += "-test"

app = modal.App(
    "save-results" + suffix,
    image=modal.Image.debian_slim().pip_install(
        "pandas~=1.5.0",
        "psycopg2-binary~=2.9.0",
        "sil-machine~=0.9.0",
        "sil-thot~=3.4.0",
        "sqlalchemy~=1.4.0",
    ),
)

RESULTS_DIR = "/results"


@app.function(network_file_systems={RESULTS_DIR: word_alignment_results_volume})
def save_results(
    revision: int,
    reference: int,
    top_scores_df,
    database_id: str,
    source_type: str = "source",
):
    """
    Save the word alignment results to the results directory in the modal shared volume.
    This function saves the word alignment top_source_scores contained in the input
    dataframe to a csv file in the results directory in the modal shared volume. The
    directory structure is created if it doesn't exist, and the file is saved as
    'top_source_scores.csv'.
    Parameters:
    revision (int): Revision id for the word alignment assessment.
    reference (int): Reference revision id for the word alignment assessment.
    source_type: Type of source used. Can be 'source' or 'target'.
    top_source_scores_df (pandas.DataFrame): Dataframe containing the word alignment
    results with columns 'vref', 'source' and 'score'.
    Returns:
    None
    """
    # Create the results directory if it doesn't exist
    results_dir = Path(RESULTS_DIR)
    results_dir.mkdir(parents=True, exist_ok=True)
    results_dir = results_dir / f"{database_id}/{reference}-{revision}"
    results_dir.mkdir(parents=True, exist_ok=True)
    print(top_scores_df.head())
    top_scores_df.to_csv(results_dir / f"top_{source_type}_scores.csv", index=False)
    print(
        f'Saved top_{source_type}_scores to {results_dir / f"top_{source_type}_scores.csv"}'
    )


@app.function(network_file_systems={RESULTS_DIR: word_alignment_results_volume})
def get_results(
    revision: int, reference: int, database_id: str, source_type: str = "source"
):
    """
    Get top_source_scores from word alignment between revision and reference.
    This function retrieves the word alignment top_source_scores from the modal shared
    volume, if it exists, otherwise it starts a word alignment assessment and waits for
    it to finish. The results are then read from the file 'top_source_scores.csv' and
    returned as a pandas dataframe.
    Parameters:
    revision (int): Revision id for the word alignment assessment.
    reference (int): Reference revision id for the word alignment assessment.
    source_type: Type of source to use for word alignment. Can be 'source' or 'target'.
    Returns:
    pandas.DataFrame: Dataframe containing the word alignment results with columns
    'vref', 'source' and 'score'.
    or None if the dataframe does not exist.
    """
    import pandas as pd

    results_dir = Path(RESULTS_DIR)
    top_scores_file = (
        results_dir
        / f"{database_id}/{reference}-{revision}"
        / f"top_{source_type}_scores.csv"
    )
    if not top_scores_file.exists():
        print(f"No top {source_type} scores for {reference}-{revision}, returning None")
        return None

    try:
        top_scores_df = pd.read_csv(top_scores_file, keep_default_na=False)
    except pd.errors.EmptyDataError:
        print(
            f"{source_type} scores for {reference}-{revision} exists, and is empty, returning Empty Dataframe"
        )
        return pd.DataFrame(columns=["vref", "source", "target", "total_score"])
    print(f"Returning top {source_type} scores for {reference}-{revision}")
    return top_scores_df


@app.function(network_file_systems={RESULTS_DIR: word_alignment_results_volume})
def delete_results(
    revision: int, reference: int, database_id: str, source_type: str = "source"
):
    """
    Delete top_source_scores from word alignment between revision and reference.
    This function deletes the word alignment top_source_scores from the modal shared
    volume.
    Parameters:
    revision (int): Revision id for the word alignment assessment.
    reference (int): Reference revision id for the word alignment assessment.
    Returns:
    None
    """
    results_dir = Path(RESULTS_DIR)
    top_scores_file = (
        results_dir
        / f"{database_id}/{reference}-{revision}"
        / f"top_{source_type}_scores.csv"
    )
    if top_scores_file.exists():
        top_scores_file.unlink()
        print(f"Deleted top source scores for {reference}-{revision}")
