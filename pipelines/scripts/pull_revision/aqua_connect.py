__version__ = '0.102'

import os
from dotenv import load_dotenv
load_dotenv()
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import contextlib


@contextlib.contextmanager
def get_aqua_conn():
    conn_string = get_connection_string()
    try:
        engine = create_engine(conn_string)
        conn = engine.connect()
        yield conn
        conn.close()
    except OperationalError as oe:
        print(oe)
        yield None

def get_connection_string():
    try:
        user = os.environ['user']
        pword = os.environ['aqua_pw']
        host  = os.environ['host']
        port = os.environ['port']
        db = os.environ['db']
        return  f"postgresql://{user}:{pword}@{host}:{port}/{db}?sslmode=require"
    except KeyError as err:
        err_message = f'Environmental variable {err} missing'
        raise KeyError(err_message)

if __name__ == '__main__':
    with get_aqua_conn() as conn:
        print(conn)