def get_session(aqua_connection_string):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    #??? Should this yield only session?
    #Need engine in testing but maybe I can pull out of session?
    try:
        engine = create_engine(aqua_connection_string, pool_size=5, pool_recycle=3600)
    except KeyError as err:
        raise KeyError(f'Missing environmental variable {err}') from err
    with Session(engine) as session:
        yield engine,session
