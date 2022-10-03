import pytest
import argparse
from mock import patch
from sem_sim import SemanticSimilarity

@pytest.mark.parametrize('target,reference,out',
                        [(None,'.','.'),
                         ('.', None, '.'),
                         ('.','.',None)], ids=['target','reference','out'])
#test for missing path
def test_missing_path(target, reference, out):
    try:
        with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(target=target,reference=reference,out=out)):
            ss = SemanticSimilarity()
        raise AssertionError('Missing path should have failed')
    except ValueError as err:
        if err.args[0] == 'Missing argument path':
            pass
        else:
            raise AssertionError(f'Error is {err}') from err