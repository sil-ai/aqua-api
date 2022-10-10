import re
import pandas as pd
import pytest
from split_revision import SplitRevision
from mock import patch
import argparse
from datetime import datetime

#gets the args of the form --input /path/to/my/input/file --num 100 --out /path/to/output

@pytest.fixture(scope='session')
def revision_filepath():
    return '2_3_merge.csv'

@pytest.mark.parametrize('input,num,out',
                        [(None,100,'.'),
                         ('./input.txt',None,'.'),
                         ('./input.txt',5,None)],
                          ids=['input','num','out'])
#test for missing path
def test_missing_path(input, num, out):
    try:
        with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(input=input, num=num, out=out)):
            SplitRevision()
        raise AssertionError('Missing path should have failed')
    except ValueError as err:
        assert err.args[0] == 'Missing split number or path', f'Error is {err}'

#test for invalid input file
def test_invalid_input():
    try:
        with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(input='./input.txt', num=5, out='.')):
            SplitRevision()
        raise AssertionError('Invalid filename should have failed')
    except FileNotFoundError as err:
        pass

def get_args():
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    return {'input': args.input,'out': args.out,'num': args.num}

def test_split_revision_instance(revision_filepath, valuestorage):
    try:
        with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(input=revision_filepath, num=100, out='.')):
            sr = SplitRevision()
            args = get_args()
            #assert that the args are correct in the instance
            assert sr.input_filepath == args['input']
            assert sr.output_filepath == args['out']
            assert sr.num == args['num']
            #assert that the revision_list file transferred properly after taking out blanks
            revision_file = pd.read_csv(revision_filepath)
            assert len(sr.revision_df) == len(revision_file)
            #save this instance to valuestorage for further tests
            valuestorage.valid_split_rev = sr
    except Exception as err:
        raise AssertionError(f'Error is {err}') from err

#test split revision
def test_split_revision(valuestorage):
   try:
        #get the split revision instance from localstorage
        sr = valuestorage.valid_split_rev
        #split the revision into chunks
        split_revision = sr.split_revision()
        #make sure that there are sr.num chunks
        assert len(split_revision) == sr.num
        #make sure the individual chunks add up to the total number of the verses
        #in the revision
        assert sum([len(item)for item in split_revision]) == len(sr.revision_df)
        #make sure that the chunks are the same size
        average_chunk_length = len(sr.revision_df)//sr.num
        assert all([(len(item)==average_chunk_length)\
               or (len(item)==average_chunk_length+1)\
                  for item in split_revision])
   except Exception as err:
        raise AssertionError(f'Error is {err}') from err

#test split revision output
def test_split_revision_output(valuestorage, tmp_path):
    try:
        #get the instance
        sr = valuestorage.valid_split_rev
        #redirect output to tmp_path which is system-generated and temporary of course!
        sr.output_filepath = tmp_path
        #create the split output files
        sr.output_split_revisions(sr.split_revision())
        list_of_chunked_files = list(tmp_path.iterdir())
        #make sure there are the correct number of files
        assert len(list_of_chunked_files) == sr.num
        #check the type of all the files is csv
        assert all([str(item).split('.')[-1]=='csv' for item in list_of_chunked_files])
        valuestorage.list_of_chunked_files = list_of_chunked_files
    except Exception as err:
        raise AssertionError(f'Error is {err}') from err

@pytest.mark.parametrize('chunk,line,expected_ref,expected_text',
  [(0,15,'GEN 1:16','And God made two great lights; the greater light to rule the day, and the lesser light to rule the night: he made the stars also.'),
   (42,14,'GEN 40:18','And Joseph answered and said, This is the interpretation thereof: The three baskets are three days:'),
   (81,13,'EXO 26:11','And thou shalt make fifty taches of brass, and put the taches into the loops, and couple the tent together, that it may be one.'),
   (97,25,'EXO 39:26','A bell and a pomegranate, a bell and a pomegranate, round about the hem of the robe to minister in; as the LORD commanded Moses.')],
   ids=['GEN 1:16','GEN 40:18','EXO 26:11','EXO 39:26'])
def test_chunked_file_contents(chunk,line,
                               expected_ref,
                               expected_text,
                               valuestorage):
    regex = re.compile(r'chunk(.*)\.csv')
    list_of_chunked_files = valuestorage.list_of_chunked_files
    try:
        this_chunk_file = list(filter(lambda item:regex.search(str(item)).groups()[0]==str(chunk), list_of_chunked_files))[0]
        this_chunk = pd.read_csv(this_chunk_file)
        this_line = this_chunk.iloc[line]
        assert this_line['vref'] == expected_ref
        assert this_line['target'] == expected_text
        assert this_line['reference'] == expected_text
    except IndexError:
        raise AssertionError('Chunk {chunk} has no file output')

# #test for working logger
# def test_working_logger(caplog, valuestorage):
#     splitrev = valuestorage.valid_split_rev
#     #generate the log entry
#     splitrev.split_revision()
#     #check the logs
#     current_dt = datetime.now()
#     logentry = caplog.records[-1]
#     print(logentry)
#     #get the utc logentry
#     log_dt = datetime.fromtimestamp(logentry.created)
#     #compare the date
#     assert current_dt.date() == log_dt.date()
#     #compare the hour after converting current_dt to utc
#     assert current_dt.hour == log_dt.hour
#     #compare the minute
#     assert abs(current_dt.minute - log_dt.minute) <=1
#     #compare the logging level
#     assert logentry.levelname == 'INFO'
#     #compare the message
#     assert  'Splitting revision' in logentry.message
#     assert str(splitrev.num) in logentry.message
#     #compare the module
#     assert logentry.module == 'split_revision'
