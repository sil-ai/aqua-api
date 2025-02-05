# Import necessary modules
import os

import sqlalchemy as db
from conftest import teardown_database
from sqlalchemy.orm import sessionmaker

# Ensure that the DB session is properly initialized

engine = db.create_engine(os.getenv("AQUA_DB"))
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


db_session = TestingSessionLocal()
# Call the teardown function
teardown_database(db_session)
