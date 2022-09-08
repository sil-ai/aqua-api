import os
import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import NoSuchModuleError, OperationalError, ArgumentError, ProgrammingError

#test for valid database connection
def test_conn(engine, session, aqua_connection_string):
    #connection is up
    assert session.is_active
    #connection matches aqua_connection_string
    assert str(engine.url) == aqua_connection_string

def get_fake_conn_string(acs):
    import random
    from string import ascii_letters
    done = False
    #??? Maybe rework?
    while not done:
        idx_list = random.sample([i for i,__ in enumerate(acs)],3)
        lst = list(acs)
        for idx in idx_list:
            lst[idx]=random.choice(ascii_letters)
        fake_string = ''.join(lst)
        if fake_string != acs:
            done=True
            return fake_string

#??? Is there a way to do this with fixtures
n=3
acs = os.environ['AQUA_CONNECTION_STRING']
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
