import os
from dotenv import load_dotenv
#!!! assumes that the .env is in the scripts super folder for now
load_dotenv()
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import contextlib
from aqua_utils import get_logger

logger = get_logger(__name__)

@contextlib.contextmanager
def get_aqua_conn():
    conn_string = get_connection_string()
    try:
        engine = create_engine(conn_string)
        conn = engine.connect()
        yield conn
        conn.close()
    except OperationalError as oe:
        logger.error(oe)
        yield None

def get_connection_string():
    try:
        #TODO: see if I can build this string in the .env file
        #user = os.environ['user']
        #pword = os.environ['aqua_pw']
        #host  = os.environ['host']
        #port = os.environ['port']
        #db = os.environ['db']
        #return  f"postgresql://{user}:{pword}@{host}:{port}/{db}?sslmode=require"
        return os.environ['aqua_connection_string']
    except KeyError:
        err_message = 'Incorrect database connection string'
        raise KeyError(err_message)

if __name__ == '__main__':
    with get_aqua_conn() as conn:
        logger.info(conn)