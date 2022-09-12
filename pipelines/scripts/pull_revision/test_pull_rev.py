import os
import argparse
from datetime import datetime
import pandas as pd
from mock import patch
import json
from pull_rev import PullRevision
from db_connect import VerseText
from conftest import ValueStorage
import pickle

#test for missing revision
def test_missing_revision():
    try:
        with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(revision=None,out='.')):
            pullrev = PullRevision()
            pullrev.pull_revision()
        raise AssertionError('Missing revision should have failed')
    except ValueError as err:
        if err.args[0] == 'Missing Revision Id or output path':
            pass
        else:
            raise AssertionError(f'Error is {err}') from err


#test for invalid revision number -3
def test_invalid_revision():
    try:
        with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(revision=-3,out='.')):
            pullrev = PullRevision()
            pullrev.pull_revision()
        if pullrev.revision_text.empty:
            pass
        else:
            raise AssertionError('Invalid revision number should have failed')
    except Exception as err:
        raise AssertionError(f'Error is {err}') from err

#test for missing output
def test_missing_output():
    try:
        with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(revision=3,out='')):
            pullrev = PullRevision()
            pullrev.pull_revision()
        raise AssertionError('Invalid revision number should have failed')
    except ValueError as err:
        if err.args[0] == 'Missing Revision Id or output path':
            pass
        else:
            raise AssertionError(f'Error is {err}') from err

#test for duplicated Bible references
def test_duplicated_refs(session, revision=3, out='.'):
    try:
        with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(revision=revision,out=out)):
            revision_verses = pd.read_sql(session.query(VerseText).\
                              filter(VerseText.bibleRevision==revision)\
                              .statement, session.bind)
            assert all(item==revision for item in revision_verses.bibleRevision)
            assert len(revision_verses.verseReference) != len(set(revision_verses.verseReference))
    except Exception as err:
        raise AssertionError(f'Error is {err}') from err


#test for a valid pull_rev
def test_valid_pull_rev(valuestorage, revision=2, out='.'):
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

#test for working logger
def test_working_logger(caplog, revision=3, out='.'):
    with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(revision=revision,out=out)):
        pullrev = PullRevision()
        pullrev.pull_revision()
    #check the logs
    current_dt = datetime.now()
    for logentry in caplog.records:
        print(logentry)
        #get the utc logentry
        log_dt = datetime.fromtimestamp(logentry.created)
        #compare the date
        assert current_dt.date() == log_dt.date()
        #compare the hour after converting current_dt to utc
        assert current_dt.hour == log_dt.hour
        #compare the minute
        assert abs(current_dt.minute - log_dt.minute) <=1
        #compare the logging level
        assert logentry.levelname == 'INFO'
        #compare the message
        assert  'Revision 3' in logentry.message
        #compare the module
        assert logentry.module == 'pull_rev'

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
    #??? What should the assertion be here?

#test for matching output
def test_matching_output(valuestorage):
    pr = valuestorage.valid_pull_rev
    valid_output = get_valid_file(valuestorage)
    #json.dump(valid_output, open('vo.json','w'))
    #pickle.dump(pr, open('pr.pkl','wb'))
    #TODO: figure out why this assertion is failing by one
    assert len(pr.vref) == len(valid_output)



