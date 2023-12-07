from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from dotenv import load_dotenv
import os

from db_service.models import Base

# Load environment variables from .env file
load_dotenv()

# Construct the DATABASE_URL from the environment variables
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

DATABASE_URL = (
    f"postgresql://"
    f"{DB_USER}:"
    f"{DB_PASS}@"
    f"{DB_HOST}:"
    f"{DB_PORT}/"
    f"{DB_NAME}"
)
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
