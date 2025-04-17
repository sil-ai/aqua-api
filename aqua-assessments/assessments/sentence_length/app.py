import os
import re
import string
from pathlib import Path
from typing import Literal, Optional, Union

# from modal import Function
import modal
from pydantic import BaseModel

# Manage deployment suffix on modal endpoint if testing.
suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix += "-test"


image_envs = {k: v for k, v in os.environ.items() if k.startswith("MODAL_")}

app = modal.App(
    "sentence-length" + suffix,
    image=modal.Image.debian_slim()
    .apt_install("libpq-dev", "gcc")
    .pip_install(
        "pandas~=1.5.0",
        "psycopg2-binary~=2.9.0",
        "pydantic~=1.10.0",
        "sqlalchemy~=1.4.0",
    )
    .copy_mount(
        mount=modal.Mount.from_local_file(
            local_path=Path("../../fixtures/vref.txt"),
            remote_path=Path("/root/vref.txt"),
        )
    )
    .env(image_envs),
)

run_pull_revision = modal.Function.lookup("pull-revision" + suffix, "pull_revision")
run_push_results = modal.Function.lookup("push-results" + suffix, "push_results")


# @app.function()
def get_vrefs():
    with open("/root/vref.txt", "r") as f:
        vrefs = f.readlines()
    vrefs = [vref.strip() for vref in vrefs]
    return vrefs


@app.function()
def get_words_per_sentence(text):
    # if text contains only spaces, return 0
    if text.isspace():
        return 0
    # get score
    words = text.split(" ")
    # sentences = text.split('.')
    sentences = re.split(
        r"\.|\?|\!|՜|՞|։|߹|܂|܁|܀|۔|؟|।|॥|།|༎|၊|။|።|፧|។|៕|⳾|。|꓿|꘎|꘏|︒|︕|︖", text
    )
    avg_words = len(words) / len(sentences)

    # round to 2 decimal places
    # avg_words = round(avg_words, 2)

    return avg_words


@app.function()
def get_long_words(text, n=7):
    # get % of words that are >= n characters
    # if text contains only spaces, return 0
    if text.isspace():
        return 0

    # clean text
    text = text.translate(str.maketrans("", "", string.punctuation))

    # get score
    words = text.split(" ")
    long_words = [word for word in words if len(word) > n]
    percent_long_words = float(len(long_words) / len(words))
    return percent_long_words * 100


class Assessment(BaseModel):
    id: Optional[int] = None
    revision_id: int
    type: Literal["sentence-length"]


# run the assessment
# for now, use the Lix formula
@app.function(cpu=8, timeout=5000, retries=3)
def assess(assessment_config: Union[Assessment, dict], AQUA_DB: str, **kwargs):
    import pandas as pd

    if isinstance(assessment_config, dict):
        assessment_config = Assessment(**assessment_config)
    # pull the revision
    rev_num = assessment_config.revision_id
    lines = run_pull_revision.remote(rev_num, AQUA_DB)
    lines = [line.strip() for line in lines]

    assert len(lines) == 41899

    # get vrefs
    vrefs = get_vrefs()

    df = pd.DataFrame({"vref": vrefs, "verse": lines})

    # replace <range> with blank
    df["verse"] = df["verse"].replace("<range>", "")

    # remove rows with no text
    df = df[df["verse"] != ""]

    # get book, chapter, and verse columns
    df["book"] = df["vref"].str.split(" ").str[0]
    df["chapter"] = df["vref"].str.split(" ").str[1].str.split(":").str[0]

    # group by book and chapter
    chapter_df = df.groupby(["book", "chapter"]).agg({"verse": " ".join}).reset_index()

    # calculate lix score for each chapter
    # chapter_df["wps"] = chapter_df["verse"].apply(get_words_per_sentence)
    chapter_df["wps"] = chapter_df["verse"].apply(
        lambda verse: get_words_per_sentence.remote(verse)
    )
    # chapter_df["percent_long_words"] = chapter_df["verse"].apply(get_long_words)
    chapter_df["percent_long_words"] = chapter_df["verse"].apply(
        lambda verse: get_long_words.remote(verse)
    )
    chapter_df["lix_score"] = chapter_df["wps"] + chapter_df["percent_long_words"]

    # add scores to original df
    # every verse in a chapter will have the same score
    df = df.merge(
        chapter_df[["book", "chapter", "lix_score", "wps", "percent_long_words"]],
        on=["book", "chapter"],
    )

    # add to results
    results = []
    for index, row in df.iterrows():
        results.append(
            {
                "assessment_id": assessment_config.id,
                "vref": row["vref"],
                "score": row["lix_score"],
                "flag": False,
            }
        )

    return {"results": results}
