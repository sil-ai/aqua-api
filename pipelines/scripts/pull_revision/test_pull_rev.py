import argparse
import pandas as pd
from mock import patch
from datetime import datetime
from pull_rev import PullRevision
from db_connect import VerseText

#test for missing revision
def test_missing_revision():
    try:
        with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(revision=None,out='.')):
            pr = PullRevision()
            pr.pull_revision()
        raise AssertionError('Missing revision should have failed')
    except ValueError as err:
        if err.args[0] == 'Missing Revision Id or output path':
            pass
        else:
            raise AssertionError(f'Error is {err}')


#test for invalid revision number -3
def test_invalid_revision():
    try:
        with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(revision=-3,out='.')):
            pr = PullRevision()
            pr.pull_revision()
        if pr.revision_text.empty:
            pass
        else:
            raise AssertionError('Invalid revision number should have failed')
    except Exception as err:
        raise AssertionError(f'Error is {err}')

#test for missing output
def test_missing_output():
    try:
        with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(revision=3,out='')):
            pr = PullRevision()
            pr.pull_revision()
        raise AssertionError('Invalid revision number should have failed')
    except ValueError as err:
        if err.args[0] == 'Missing Revision Id or output path':
            pass
        else:
            raise AssertionError(f'Error is {err}')

#test for duplicated Bible references
def test_duplicated_refs(session, revision=3, out='.'):
    try:
        with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(revision=revision,out=out)):
            revision_verses = pd.read_sql(session.query(VerseText).filter(VerseText.bibleRevision==revision).statement, session.bind)
            assert all([item==revision for item in revision_verses.bibleRevision])
            assert len(revision_verses.verseReference) != len(set(revision_verses.verseReference))
    except Exception as err:
        raise AssertionError(f'Error is {err}')


#test for a valid pull_rev
def test_valid_pull_rev(revision=2, out='.'):
    try:
        with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(revision=revision,out=out)):
            pr = PullRevision()
            pr.pull_revision()
            pr.output_revision()
        assert pr.revision_id == revision
        assert pr.out == out
        assert len(pr.revision_text) > 0
        assert all([item==revision for item in pr.revision_text.bibleRevision])
    except Exception as err:
        raise AssertionError(f'Error is {err}')

#test for working logger
def test_working_logger(caplog, revision=3, out='.'):
    with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(revision=revision,out=out)):
        pr = PullRevision()
        pr.pull_revision()
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
        assert 'pull_rev' == logentry.module