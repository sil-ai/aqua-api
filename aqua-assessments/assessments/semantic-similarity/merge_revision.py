import logging

import pandas as pd


logging.getLogger().setLevel("INFO")


def condense_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes an input dataframe with revision and reference columns, and outputs
    a dataframe which only include those lines that are not blank in both input files.
    Also condenses <range> lines into the previous line in both revision and reference, 
    and removes the vref for that line, adding the removed indices into the 'indices'
    column, so you know which indices have been combined.

    Inputs:
    df                 A dataframe with vref, revision and reference columns

    Outputs:
    df                  The condensed dataframe

    """
    df["indices"] = df.index
    df.loc[:, "indices"] = df["indices"].apply(lambda x: str(x))
    df.loc[:, "reference"] = df["reference"].apply(lambda x: str(x))
    df.loc[:, "revision"] = df["revision"].apply(lambda x: str(x))
    df = df[(df["reference"] != "\n") & (df["reference"] != "")]
    df = df[(df["revision"] != "\n") & (df["revision"] != "")]
    df["next_reference"] = df["reference"].shift(-1)
    df["next_revision"] = df["revision"].shift(-1)
    df["range_next"] = (df["next_reference"] == "<range>") | (
        df["next_revision"] == "<range>"
    )
    df = df.reset_index()
    for index, row in df[:1:-1].iterrows():
        if row["range_next"]:
            df.loc[index, "indices"] += " " + df.loc[index + 1, "indices"]
            if len(df.loc[index + 1, "reference"].replace("<range>", "")) > 0:
                df.loc[index, "reference"] += " " + df.loc[
                    index + 1, "reference"
                ].replace("<range>", "")
            if len(df.loc[index + 1, "revision"].replace("<range>", "")) > 0:
                df.loc[index, "revision"] += " " + df.loc[
                    index + 1, "revision"
                ].replace("<range>", "")
    df = df[(df["reference"] != "<range>") & (df["revision"] != "<range>")]
    df = df.drop(["next_reference", "next_revision", "range_next"], axis=1)
    df = df.set_index("vref")
    return df


class MergeRevision:
    def __init__(self, revision_id, revision_verses, reference_id, reference_verses):
        self.revision_id = revision_id
        self.revision = revision_verses
        self.reference_id = reference_id
        self.reference = reference_verses
        self.vref = open("/root/vref.txt").read().splitlines()

    def check_matching_length(self):
        return len(self.revision) == len(self.reference)

    def check_vref(self):
        return (len(self.revision) == len(self.vref)) and (
            len(self.reference) == len(self.vref)
        )

    def merge_revision(self):
        # check that draft and reference are the same length
        if not self.check_matching_length():
            raise ValueError(
                f"draft and reference differ by {abs(len(self.reference)- len(self.revision))}"
            )
        # check that both draft and reference are the same length as vref
        elif not self.check_vref():
            raise ValueError("draft and/or reference length don't match vref")
        else:
            # merge the two revisions together
            merged_revisions = pd.DataFrame(
                {"revision": self.revision, "reference": self.reference},
                index=self.vref,
            )
            merged_revisions.index.name = "vref"
            merged_revisions = condense_df(merged_revisions)
            logging.info(
                f"Revision {self.revision_id} and {self.reference_id} are merged"
            )
            logging.info(merged_revisions.head(10))
            return merged_revisions
