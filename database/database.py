import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from database.models import Base

# Load environment variables from .env file
load_dotenv()

DATABASE_URL = os.getenv("AQUA_DB")


# If the DATABASE_URL is not set, we should raise an exception
if not DATABASE_URL:
    raise ValueError("No DATABASE_URL.")

engine = create_engine(DATABASE_URL)
db_session = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine)
)


Base.query = db_session.query_property()


def init_db():
    # Import all modules here that might define models so that
    # they will be registered properly on the metadata.
    Base.metadata.create_all(bind=engine)
