import os
import argparse
import pytest
from mock import patch
from switchboard import ApiSwitchboard
from assessment_operations import GetAssessment

@pytest.mark.parametrize('target,reference,assess_type',
                        [(None,None,'subwords'),
                         (None,1,'')], ids=['No ref','No assess_type'])
#test for missing arguments
def test_missing_args(target, reference, assess_type):
    try:
        with patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(target=target,
                                            ref=reference,
                                            assess_type=assess_type)):
            ApiSwitchboard()
        raise AssertionError('Missing args should have failed')
    except ValueError as err:
        assert err.args[0] == 'Missing args'

@patch('argparse.ArgumentParser.parse_args',
                   return_value=argparse.Namespace(target=None,
                                                   ref=1,
                                                   assess_type='semsim'))
#tests for invalid semsim
def test_missing_semsim_target(mock_args):
    try:
        ApiSwitchboard()
        raise AssertionError('Missing target should have failed')
    except ValueError as err:
        assert err.args[0] == 'Semsim assessment type requires a valid target revision'

#tests for valid args
@patch('argparse.ArgumentParser.parse_args',
            return_value = argparse.Namespace(target=1, ref=2, assess_type='semsim'))
def test_valid_switchboard(mock_args, valuestorage):
    switch = ApiSwitchboard()
    assert type(switch.target) == int
    assert switch.target == mock_args.return_value.target
    assert type(switch.ref) == int
    assert switch.ref == mock_args.return_value.ref
    assert switch.assess_type == 'semsim'
    #save valid switchboard for further tests
    valuestorage.valid_switchboard = switch

#job id is updated for valid job
def test_valid_job(valuestorage):
    switch_object = valuestorage.valid_switchboard
    assert type(switch_object.job_id) == int
    assert switch_object.job_id == int(open('counter.txt').read()) + 1
    #update the counter file by switching
    switch_object.switch()
    #check the counter file
    assert switch_object.job_id == int(open('counter.txt').read())

@pytest.mark.parametrize('target, ref, assess_type',
                         [(1, -2, 'semsim'),
                          (1, 5000, 'semsim')],
                         ids=['negative_ref','large ref id']
                         )
#job id is not updated for invalid job
def test_invalid_job(target, ref, assess_type, valuestorage):
    switch_object = valuestorage.valid_switchboard
    with patch('argparse.ArgumentParser.parse_args',
            return_value = argparse.Namespace(target=target,
                                              ref=ref,
                                              assess_type=assess_type)):
        try:
            switch = ApiSwitchboard().switch()
            raise AssertionError('Issue with Database integrity checks')
        except ValueError as err:
            #should have a database error
            assert err.args[0] == 'Problem with database values'
            #how about job_id not updating?
            assert switch_object.job_id == int(open('counter.txt').read())

#valid job is stored in Assessment folder
def test_stored_job(valuestorage):
    switch_object = valuestorage.valid_switchboard
    result = GetAssessment().get_assessment(switch_object.job_id)
    #job id assertion passes by nature of the query
    assert switch_object.target == result.revision
    assert switch_object.ref == result.reference
    #!!! value depends on valuestorage above currently semsim
    assert switch_object.assess_type == result.type == 'semsim'
    assert result.finished == False
