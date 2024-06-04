# version_id conftest.py

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from database.models import (
    Base,
    UserDB,
    Group,
    UserGroup,
    IsoLanguage,
    IsoScript,
    BibleVersion,
    BibleRevision,
    BookReference,
    ChapterReference,
    VerseReference,
)
import bcrypt
from datetime import date
import pandas as pd
from app import app
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

engine = create_engine("postgresql://dbuser:dbpassword@localhost:5432/dbname")
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Asynchronous session fixture
@pytest.fixture(scope="module")
async def async_test_db_session():
    async_engine = create_async_engine("postgresql+asyncpg://dbuser:dbpassword@localhost:5432/dbname")
    AsyncSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=async_engine, class_=AsyncSession)
    async with AsyncSessionLocal() as async_session:
        yield async_session
   
@pytest.fixture(scope="module")
def db_session():
    return TestingSessionLocal()


@pytest.fixture(scope="module")
def client():
    return TestClient(app)


@pytest.fixture(scope="module")
def regular_token1(client, test_db_session):
    response = client.post(
        "/latest/token", data={"username": "testuser1", "password": "password1"}
    )
    return response.json().get("access_token")


@pytest.fixture(scope="module")
def regular_token2(client, test_db_session):
    response = client.post(
        "/latest/token", data={"username": "testuser2", "password": "password2"}
    )
    return response.json().get("access_token")


@pytest.fixture(scope="module")
def admin_token(client, test_db_session):
    response = client.post(
        "/latest/token", data={"username": "admin", "password": "adminpassword"}
    )
    return response.json().get("access_token")


@pytest.fixture(scope="module")
def test_db_session():
    Base.metadata.create_all(bind=engine)
    db_session = TestingSessionLocal()

    # Add your test data setup here
    setup_database(db_session)
    try:
        yield db_session

    # Teardown test data
    finally:
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
    # Create users
    test_user1 = UserDB(
        username="testuser1",
        email="testuser1@example.com",
        hashed_password=bcrypt.hashpw("password1".encode(), bcrypt.gensalt()).decode(),
        is_admin=False,
    )
    test_user2 = UserDB(
        username="testuser2",
        email="testuser2@example.com",
        hashed_password=bcrypt.hashpw("password2".encode(), bcrypt.gensalt()).decode(),
        is_admin=False,
    )
    admin_user = UserDB(
        username="admin",
        email="admin@example.com",
        hashed_password=bcrypt.hashpw(
            "adminpassword".encode(), bcrypt.gensalt()
        ).decode(),
        is_admin=True,
    )

    db_session.add_all([test_user1, test_user2, admin_user])
    db_session.commit()

    # Create groups
    group1 = Group(name="Group1", description="Test Group 1")
    group2 = Group(name="Group2", description="Test Group 2")

    db_session.add_all([group1, group2])
    db_session.commit()

    # Associate users with groups
    user_group1 = UserGroup(user_id=test_user1.id, group_id=group1.id)
    user_group2 = UserGroup(user_id=test_user2.id, group_id=group2.id)

    db_session.add_all([user_group1, user_group2])
    db_session.commit()


async def setup_users_and_groups_async(session):
    """Setup test users and groups asynchronously."""
    # Create users
    test_user1 = UserDB(
        username="testuser1",
        email="testuser1@example.com",
        hashed_password=bcrypt.hashpw("password1".encode(), bcrypt.gensalt()).decode(),
        is_admin=False,
    )
    test_user2 = UserDB(
        username="testuser2",
        email="testuser2@example.com",
        hashed_password=bcrypt.hashpw("password2".encode(), bcrypt.gensalt()).decode(),
        is_admin=False,
    )
    admin_user = UserDB(
        username="admin",
        email="admin@example.com",
        hashed_password=bcrypt.hashpw(
            "adminpassword".encode(), bcrypt.gensalt()
        ).decode(),
        is_admin=True,
    )

    session.add_all([test_user1, test_user2, admin_user])
    await session.commit()  # Use await for committing the transaction

    # Create groups
    group1 = Group(name="Group1", description="Test Group 1")
    group2 = Group(name="Group2", description="Test Group 2")

    session.add_all([group1, group2])
    await session.commit()  # Use await for committing the transaction

    # Associate users with groups
    user_group1 = UserGroup(user_id=test_user1.id, group_id=group1.id)
    user_group2 = UserGroup(user_id=test_user2.id, group_id=group2.id)

    session.add_all([user_group1, user_group2])
    await session.commit()  # Use await for committing the transaction



def setup_references_and_isos(db_session):
    """Setup reference data and ISO codes."""
    # Load data from CSV files
    book_ref_df = pd.read_csv("fixtures/book_reference.txt", sep="\t")
    chapter_ref_df = pd.read_csv("fixtures/chapter_reference.txt", sep="\t")
    verse_ref_df = pd.read_csv("fixtures/verse_reference.txt", sep="\t")

    # Populate book_reference, chapter_reference, verse_reference tables
    for _, row in book_ref_df.iterrows():
        db_session.add(BookReference(**row.to_dict()))
    for _, row in chapter_ref_df.iterrows():
        db_session.add(ChapterReference(**row.to_dict()))
    for _, row in verse_ref_df.iterrows():
        db_session.add(VerseReference(**row.to_dict()))

    # Add ISO language and script
    db_session.add(IsoLanguage(iso639="eng", name="english"))
    db_session.add(IsoLanguage(iso639="ngq", name="ngq"))
    db_session.add(IsoLanguage(iso639="swh", name="swh"))
    db_session.add(IsoScript(iso15924="Latn", name="latin"))
    db_session.commit()


async def setup_references_and_isos_async(session):
    """Setup reference data and ISO codes asynchronously."""
    # Load data from CSV files
    book_ref_df = pd.read_csv("fixtures/book_reference.txt", sep="\t")
    chapter_ref_df = pd.read_csv("fixtures/chapter_reference.txt", sep="\t")
    verse_ref_df = pd.read_csv("fixtures/verse_reference.txt", sep="\t")

    # Populate book_reference, chapter_reference, verse_reference tables
    book_refs = [BookReference(**row.to_dict()) for _, row in book_ref_df.iterrows()]
    chapter_refs = [ChapterReference(**row.to_dict()) for _, row in chapter_ref_df.iterrows()]
    verse_refs = [VerseReference(**row.to_dict()) for _, row in verse_ref_df.iterrows()]

    session.add_all(book_refs + chapter_refs + verse_refs)

    # Add ISO language and script
    iso_languages = [
        IsoLanguage(iso639="eng", name="english"),
        IsoLanguage(iso639="ngq", name="ngq"),
        IsoLanguage(iso639="swh", name="swh")
    ]
    iso_scripts = [
        IsoScript(iso15924="Latn", name="latin")
    ]

    session.add_all(iso_languages + iso_scripts)
    await session.commit()  # Use await for committing the transaction


def load_revision_data(db_session):
    """Load revision data into the database."""
    # Add version
    # query the id for testuser1
    user = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    user_id = user.id if user else None

    version = BibleVersion(
        name="loading_test",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="BLTEST",
        owner_id=user_id,
        is_reference=False
    )
    db_session.add(version)

    # Commit to save the version and retrieve its ID for the revision
    db_session.commit()

    # Add revision
    revision = BibleRevision(
        date=date.today(),
        bible_version_id=version.id,
        published=False,
        machine_translation=True,
    )
    db_session.add(revision)
    db_session.commit()


async def load_revision_data_async(session):
    """Load revision data into the database asynchronously."""
    # Add version
    # Asynchronously query the id for testuser1
    result = await session.execute(
        select(UserDB).where(UserDB.username == "testuser1")
    )
    user = result.scalars().first()
    user_id = user.id if user else None

    version = BibleVersion(
        name="loading_test",
        iso_language="eng",
        iso_script="Latn",
        abbreviation="BLTEST",
        owner_id=user_id,
    )
    session.add(version)

    # Commit to save the version and retrieve its ID for the revision
    await session.commit()

    # Add revision
    revision = BibleRevision(
        date=date.today(),
        bible_version_id=version.id,
        published=False,
        machine_translation=True,
    )
    session.add(revision)
    await session.commit()


def teardown_database(db_session):
    engine = db_session.get_bind()
    with engine.connect() as connection:
        with connection.begin() as transaction:
            connection.execute("SET session_replication_role = replica;")
            for table_name in reversed(Base.metadata.sorted_tables):
                connection.execute(table_name.delete())
            connection.execute("SET session_replication_role = DEFAULT;")
            transaction.commit()


async def teardown_database_async(session):
    """
    Tear down the database by deleting all data from tables asynchronously, 
    using session and engine from async ORM.
    """
    async with session.begin():
        session.execute("SET session_replication_role = replica;")
        for table in reversed(Base.metadata.sorted_tables):
            await session.execute(table.delete())
        session.execute("SET session_replication_role = DEFAULT;")
        
        
if __name__ == "__main__":
    setup_database(TestingSessionLocal())