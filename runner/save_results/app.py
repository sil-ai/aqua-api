from pathlib import Path
import os

import modal


word_alignment_results_volume = modal.SharedVolume().persist("word_alignment_results")


# Manage suffix on modal endpoint if testing.
suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix = "-test"

stub = modal.Stub(
    "save-results" + suffix,
    image=modal.Image.debian_slim()
    .pip_install(
        "pandas==1.4.3",
        "machine==0.0.1",
        "sil-machine[thot]>=0.8.3",
        "asyncio",
        "sqlalchemy",
        "psycopg2-binary",
        "requests_toolbelt==0.9.1",
    )
)

RESULTS_DIR = Path("/results")


@stub.function(
    shared_volumes={RESULTS_DIR: word_alignment_results_volume},
    secret=modal.Secret.from_name("aqua-db"),
)
def save_results(revision: int, reference: int, top_source_scores_df, database_id: str):
    """
    Save the word alignment results to the results directory in the modal shared volume.

    This function saves the word alignment top_source_scores contained in the input 
    dataframe to a csv file in the results directory in the modal shared volume. The 
    directory structure is created if it doesn't exist, and the file is saved as 
    'top_source_scores.csv'.

    Parameters:
    revision (int): Revision id for the word alignment assessment.
    reference (int): Reference revision id for the word alignment assessment.
    top_source_scores_df (pandas.DataFrame): Dataframe containing the word alignment 
    results with columns 'vref', 'source' and 'score'.

    Returns:
    None
    """
    # Create the results directory if it doesn't exist
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    results_dir = RESULTS_DIR / f"{database_id}/{reference}-{revision}"
    results_dir.mkdir(parents=True, exist_ok=True)
    top_source_scores_df.to_csv(results_dir / "top_source_scores.csv", index=False)
    print(f'Saved top_source_scores to {results_dir / "top_source_scores.csv"}')


@stub.function(
    shared_volumes={RESULTS_DIR: word_alignment_results_volume},
)
def get_results(revision: int, reference: int, database_id: str):
    """
    Get top_source_scores from word alignment between revision and reference.

    This function retrieves the word alignment top_source_scores from the modal shared 
    volume, if it exists, otherwise it starts a word alignment assessment and waits for 
    it to finish. The results are then read from the file 'top_source_scores.csv' and 
    returned as a pandas dataframe.

    Parameters:
    revision (int): Revision id for the word alignment assessment.
    reference (int): Reference revision id for the word alignment assessment.

    Returns:
    pandas.DataFrame: Dataframe containing the word alignment results with columns 
    'vref', 'source' and 'score'.

    or None if the dataframe does not exist.
    """
    import pandas as pd

    top_source_scores_file = (
        RESULTS_DIR / f"{database_id}/{reference}-{revision}" / "top_source_scores.csv"
    )
    if not top_source_scores_file.exists():
        return None

    top_source_scores_df = pd.read_csv(top_source_scores_file)
    print(f'Returning top source scores for {reference}-{revision}')
    return top_source_scores_df


@stub.function(
    shared_volumes={RESULTS_DIR: word_alignment_results_volume},
    # secrets=[modal.Secret.from_name("aqua-db"), modal.Secret.from_name("aqua-api")],
)
def delete_results(revision: int, reference: int, database_id: str):
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
    top_source_scores_file = (
        RESULTS_DIR / f"{database_id}/{reference}-{revision}" / "top_source_scores.csv"
    )
    if top_source_scores_file.exists():
        top_source_scores_file.unlink()
        print(f'Deleted top source scores for {reference}-{revision}')