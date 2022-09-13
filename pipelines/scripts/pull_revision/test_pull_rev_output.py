import os
import argparse
from pull_rev import PullRevision
from mock import patch
from datetime import datetime
import pandas as pd

#test for a valid pull_rev
def test_valid_pull_rev(valuestorage, revision=2, out='out'):
    try:
        with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(revision=revision,out=out)):
            pullrev = PullRevision()
            pullrev.pull_revision()
            pullrev.output_revision()
        assert pullrev.revision_id == revision
        assert pullrev.out == out
        assert len(pullrev.revision_text) > 0
        assert all(item==revision for item in pullrev.revision_text.bibleRevision)
        #assign the valid pullrev for later tests
        valuestorage.valid_pull_rev = pullrev
        valuestorage.revision = revision
        valuestorage.out = out
    except Exception as err:
        raise AssertionError(f'Error is {err}') from err

def get_valid_file(vs):
    #passed along from test_valid_pull_rev
    pr = vs.valid_pull_rev
    date = datetime.now().strftime("%Y_%m_%d")
    filename = f'{pr.revision_id}_{date}.txt'
    #get the correct path
    if os.curdir != pr.out:
        filepath = os.path.join(os.curdir,pr.out, filename)
    else:
        filepath = os.path.join(pr.out,filename)
    try:
        #if the file opens it exists in the correct path
        return open(filepath).read().splitlines()
    except FileNotFoundError as err:
        raise AssertionError(err) from err

#test that valid output is in correct folder
def test_output_folder(valuestorage):
    #passed along from test_valid_pull_rev
    pr = valuestorage.valid_pull_rev
    assert pr.out == valuestorage.out
    assert pr.revision_id == valuestorage.revision
    valid_output = get_valid_file(valuestorage)
    assert type(valid_output) == list

#test for matching output
def test_matching_output(valuestorage):
    pr = valuestorage.valid_pull_rev
    valid_output = get_valid_file(valuestorage)
    #check that output matches the reference list for length
    assert len(pr.vref) == len(valid_output)
    # check that trimmed output matches for length
    non_empty = [item for item in valid_output if item]
    assert len(pr.revision_text) == len(non_empty)
    #check that the references and text line up in a trimmed zipped file and match revision_text
    trimmed_output = [item for item in list(zip(pr.vref,valid_output)) if item[1]]
    trimmed_output_df = pd.DataFrame(trimmed_output, columns=['verseReference','text'])
    merged_output = pd.merge(pr.revision_text,trimmed_output_df, on=['verseReference','text'], how='outer')
    #all verses line up and references line up otherwise lengths will be different
    assert len(merged_output) == len(trimmed_output_df)
    