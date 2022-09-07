import pytest
import os
from mock import patch
import argparse
from sqlalchemy import create_engine
from sqlalchemy.exc import NoSuchModuleError, OperationalError, ArgumentError, ProgrammingError
from pull_rev import PullRevision
import dateutil.parser as p
import pandas as pd
from datetime import datetime
from aqua_connect import VerseText

#test for valid database connection
def test_conn(engine, session, aqua_connection_string):
    #connection is up
    assert session.is_active
    #connection matches aqua_connection_string
    assert str(engine.url) == aqua_connection_string

def get_fake_conn_string(aqua_connection_string):
    import random
    from string import ascii_letters
    done = False
    while not done:
        idx_list = random.sample([i for i,__ in enumerate(aqua_connection_string)],3)
        lst = list(aqua_connection_string)
        for idx in idx_list:
            lst[idx]=random.choice(ascii_letters)
        fake_string = ''.join(lst)
        if fake_string != aqua_connection_string:
            done=True
            return fake_string

#??? Is there a way to do this with fixtures
n=3
acs = os.environ['aqua_connection_string']
fake_strings = [get_fake_conn_string(acs) for __ in range(n)]

#test for n invalid database connections
@pytest.mark.parametrize("bad_connection_string",fake_strings, ids=range(1,n+1))
def test_bad_connection_string(bad_connection_string):
    #bad_connection_string = get_fake_conn_string(aqua_connection_string)
    assert bad_connection_string!= acs
    try:
        engine = create_engine(bad_connection_string)
        engine.connect()
        raise AssertionError(f'Bad connection string {bad_connection_string} worked')
    except (ValueError,
            OperationalError,
            NoSuchModuleError,
            ArgumentError,
            ProgrammingError) as err:
        #if it gets here it raised a known sqlalchemy exception
        print(f'{bad_connection_string} gives Error \n {err}')
        pass

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
def test_working_logger(revision=2, out='.'):
    with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(revision=revision,out=out)):
        pr = PullRevision()
        pr.pull_revision()
    #check the logs
    with open(pr.logger.handlers[0].baseFilename, 'r') as log_file:
        logentry = log_file.read().splitlines()[-1]
        dt, level, message, send_module = logentry.split(' | ')
    current_dt = datetime.now()
    #get the utc logentry
    log_dt = p.parse(dt).utcnow()
    #compare the date
    assert current_dt.date() == log_dt.date()
    #compare the hour after converting current_dt to utc
    assert current_dt.utcnow().hour == log_dt.hour
    #compare the minute
    assert current_dt.minute == log_dt.minute
    #compare the logging level
    assert level == 'INFO'
    #compare the message
    assert  'Loading verses' in message
    #compare the module
    assert 'pull_rev' == send_module
