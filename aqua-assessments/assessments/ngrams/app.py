import os
from pathlib import Path
import string
from typing import Literal, Optional, Union
from modal import Function
import modal
from pydantic import BaseModel

import string
from collections import defaultdict
from itertools import islice

import nltk
nltk.download('punkt')
nltk.download('punkt_tab')


# Manage deployment suffix on modal endpoint if testing.
suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix += "-test"


image_envs = {k: v for k, v in os.environ.items() if k.startswith("MODAL_")}


app = modal.App(
    "ngrams" + suffix,
    image=modal.Image.debian_slim()
    .apt_install("libpq-dev", "gcc")
    .pip_install(
        "pandas~=1.5.0",
        "psycopg2-binary~=2.9.0",
        "pydantic~=1.10.0",
        "sqlalchemy~=1.4.0",
        "tqdm~=4.66.0",
        "nltk~=3.6.2",
    )
    .copy_mount(
        mount=modal.Mount.from_local_file(
            local_path=Path("../../fixtures/vref.txt"),
            remote_path=Path("/root/vref.txt"),
        )
    )
    .env(image_envs),
)


run_pull_revision =  modal.Function.lookup(
    "pull-revision" + suffix, "pull_revision"
)
run_push_results =  modal.Function.lookup(
    "push-results" + suffix, "push_results"
)


def get_vrefs():
    with open("/root/vref.txt", "r") as f:
        vrefs = f.readlines()
    vrefs = [vref.strip() for vref in vrefs]
    return vrefs


@app.function()
def get_text(rev_id: int, AQUA_DB: str):
    return run_pull_revision.remote(rev_id, AQUA_DB)


class Assessment(BaseModel):
    id: Optional[int] = None
    revision_id: int
    min_n: Optional[int] = 2
    type: Literal["ngrams"]


@app.function()
def find_ngrams(reference_text, n):
    """
    Finds n-grams that appear more than once across multiple texts.

    Args:
        texts: A list of strings to analyze.
        n: The size of the n-grams.

    Returns:
        A list of n-grams that appear more than once across all texts.
    """
    from tqdm import tqdm
    import nltk

    ngrams = defaultdict(lambda: {
       "count": 0, 
       "vrefs": [], 
       "reference_text": []
       })

    vrefs = get_vrefs()

    # loop through each verse and find ngrams
    for i in tqdm(range(len(reference_text))):

        #only process if the reference text is not empty
        if reference_text[i] != "":

            #clean and tokenize the reference text
            cleaned_reference_text = reference_text[i].translate(str.maketrans('', '', string.punctuation+"”“’‘")).lower()
            words = nltk.word_tokenize(cleaned_reference_text)

            #get current vref
            vref = vrefs[i]

            #get all ngrams of length n
            ngrams_in_text = (tuple(islice(words, i, i + n)) for i in range(len(words) - n + 1))
            for ngram in ngrams_in_text:
                ngrams[ngram]["count"] += 1

                #only add ngram if it is not redundant
                if vref not in ngrams[ngram]["vrefs"]:
                    ngrams[ngram]["vrefs"].append(vref)
    
    # filter for ngrams that appear more than once
    result = [{
       "ngram": " ".join(ngram), 
       "vrefs": data["vrefs"], 
       } for ngram, data in ngrams.items() if data["count"] > 1]
    
    return result


@app.function()
def return_as_dict(reference_text, min_n):
    """
    Creates a dictionary of n-grams found in the reference text.
    It starts with n-grams of size min_n and continues until no n-grams are found of size min_n.

    Args:
        reference: The reference text to analyze.
        min_n: The minimum size of n-grams to find.

    Returns:
        A dictionary of n-grams found in the reference text.
    """
    ngrams_dict = {}
    ngrams_found = find_ngrams.remote(reference_text, min_n)

    print("looping through ngrams")
    while ngrams_found:
        print(f"Looking for n-grams of size {min_n}...")
        ngrams_dict[min_n] = ngrams_found
        min_n += 1
        ngrams_found = find_ngrams.remote(reference_text, min_n)

    return ngrams_dict


@app.function()
def filter_redundant_ngrams_recursive(ngrams_dict):
    """
    Filters out redundant n-grams from the n-grams dictionary.
    An n-gram is considered redundant if it is contained in a larger n-gram with the same set of vrefs.

    Args:
        ngrams_dict: A dictionary of n-grams found in the reference text.

    Returns:
        A dictionary with redundant n-grams removed.
    """
      
    # Store n-grams in descending order of size
    sorted_keys = sorted(ngrams_dict.keys(), reverse=True)

    # To track all larger n-grams for redundancy checks
    all_larger_ngrams = set()  # Store n-grams as strings for easy containment checks

    for key in sorted_keys:
        print(f"Filtering n-grams of size {key}...")
        filtered_ngrams = []
        for ngram_entry in ngrams_dict[key]:
            ngram_str = ngram_entry["ngram"]

            # # Check if n-gram is contained in any of the larger ngrams with the same set of vrefs
            if any(ngram_str in larger_ngram for larger_ngram in all_larger_ngrams):
                continue  # Skip redundant n-grams

            # Otherwise, keep the n-gram
            filtered_ngrams.append(ngram_entry)

        # Update dictionary with filtered results
        ngrams_dict[key] = filtered_ngrams

        # Add current n-grams to the larger n-grams set
        all_larger_ngrams.update(ngram_entry["ngram"] for ngram_entry in ngrams_dict[key])

    #remove empty entries
    ngrams_dict = {k: v for k, v in ngrams_dict.items() if v}  # Remove empty entries in place
    return {k: ngrams_dict[k] for k in sorted(ngrams_dict.keys(), reverse=True)}



@app.function(cpu=8, timeout=600, retries=3)
def assess(assessment: Union[Assessment, dict], AQUA_DB: str, **kwargs):
    import json

    if isinstance(assessment, dict):
        assessment = Assessment(**assessment)

    reference = get_text.remote(assessment.revision_id, AQUA_DB)

    assert len(reference) == 41899


    print("searching for ngrams")
    ngrams_dict = return_as_dict.remote(reference, min_n=assessment.min_n)

    print("filtering redundant ngrams")
    filtered_ngrams_dict = filter_redundant_ngrams_recursive.remote(ngrams_dict)

    results = []

    print("length of ngrams dict: ", len(filtered_ngrams_dict))

    # loop through ngram sizes
    for n in filtered_ngrams_dict.items():
        for item in n[1]:
            results.append(
                {
                    "assessment_id": assessment.id,
                    "ngram_size": n[0],
                    "ngram": item["ngram"],
                    "vrefs": item["vrefs"],
                }
            )

    return {"results": results}