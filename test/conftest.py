# conftest.py

import os
import pytest
import sqlalchemy as db
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database.models import Base, UserDB, Group, UserGroup, IsoLanguage, IsoScript, BibleVersion, BibleRevision, BookReference, ChapterReference, VerseReference
import bcrypt
from datetime import date
import pandas as pd
# Assuming you have an environment variable for your test database

engine = db.create_engine(os.getenv("AQUA_DB"))
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@pytest.fixture(scope="module")
def test_db_session():
    Base.metadata.create_all(bind=engine)
    db_session = TestingSessionLocal()

    # Add your test data setup here
    setup_database(db_session)

    yield db_session

    # Teardown test data
    teardown_database(db_session)
    db_session.close()

def setup_database(db_session):
    """Set up the database for testing with distinct sections for different data types."""

    # Section 1: Setting up Users and Groups
    setup_users_and_groups(db_session)

    # Section 2: Setting up References and ISOs
    setup_references_and_isos(db_session)

    # Section 3: Loading Revision
    load_revision_data(db_session)

def setup_users_and_groups(db_session):
    """Setup test users and groups."""
    test_user = UserDB(
        username="testuser",
        email="testuser@example.com",
        hashed_password=bcrypt.hashpw("password".encode(), bcrypt.gensalt()).decode(),
        is_admin=False
    )
    admin_user = UserDB(
        username="admin",
        email="admin@example.com",
        hashed_password=bcrypt.hashpw("adminpassword".encode(), bcrypt.gensalt()).decode(),
        is_admin=True
    )

    db_session.add(test_user)
    db_session.add(admin_user)
    db_session.commit()

def setup_references_and_isos(db_session):
    """Setup reference data and ISO codes."""
    # Load data from CSV files
    book_ref_df = pd.read_csv('fixtures/book_reference.txt', sep='\t')
    chapter_ref_df = pd.read_csv('fixtures/chapter_reference.txt', sep='\t')
    verse_ref_df = pd.read_csv('fixtures/verse_reference.txt', sep='\t')

    # Populate book_reference, chapter_reference, verse_reference tables
    for _, row in book_ref_df.iterrows():
        db_session.add(BookReference(**row.to_dict()))
    for _, row in chapter_ref_df.iterrows():
        db_session.add(ChapterReference(**row.to_dict()))
    for _, row in verse_ref_df.iterrows():
        db_session.add(VerseReference(**row.to_dict()))

    # Add ISO language and script
    db_session.add(IsoLanguage(iso639="eng", name="english"))
    db_session.add(IsoScript(iso15924="Latn", name="latin"))
    db_session.commit()

def load_revision_data(db_session):
    """Load revision data into the database."""
    # Add version
    version = BibleVersion(name="loading_test", iso_language="eng", iso_script="Latn", abbreviation="BLTEST")
    db_session.add(version)

    # Commit to save the version and retrieve its ID for the revision
    db_session.commit()

    # Add revision
    revision = BibleRevision(date=date.today(), bible_version_id=version.id, published=False, machine_translation=True)
    db_session.add(revision)
    db_session.commit()
   
    
def teardown_database(db_session):
    engine = db_session.get_bind()
    with engine.connect() as connection:
        with connection.begin() as transaction:
            connection.execute("SET session_replication_role = replica;")
            for table_name in reversed(Base.metadata.sorted_tables):
                connection.execute(table_name.delete())
            connection.execute("SET session_replication_role = DEFAULT;")
            transaction.commit()

