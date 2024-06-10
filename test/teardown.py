# Import necessary modules
from conftest import teardown_database
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
import os

# Ensure that the DB session is properly initialized

engine = db.create_engine(os.getenv("AQUA_DB"))
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


db_session = TestingSessionLocal()
# Call the teardown function
teardown_database(db_session)
