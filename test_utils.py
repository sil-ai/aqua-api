import os
import psycopg2
from datetime import date
import queries


def setup_database():
    """Setup database for testing."""
    connection_string = os.getenv("AQUA_DB")
    connection = psycopg2.connect(connection_string)
    cursor = connection.cursor()

    # Your setup queries
    iso_language_query = queries.add_iso_language()
    iso_script_query = queries.add_iso_script()
    version_query = queries.add_version_query()

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(iso_language_query, ("eng", "english"))
            cursor.execute(iso_script_query, ("Latn", "latin"))
            cursor.execute(
                version_query,
                ("loading_test", "eng", "Latn", "BLTEST", None, None, None, False),
            )

    # Fetch version_id and other setup steps
    fetch_version_query = queries.fetch_bible_version_by_abbreviation()
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(fetch_version_query, ("BLTEST",))
            fetch_version_data = cursor.fetchone()
            version_id = fetch_version_data[0]

    revision_date = str(date.today())
    revision_query = queries.insert_bible_revision()

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                revision_query,
                (version_id, revision_date, None, False, None, None, True),
            )
            revision_response = cursor.fetchone()
            revision_id = revision_response[0]

    connection.close()
    return revision_id


def teardown_database(revision_id):
    """Teardown database after testing."""
    connection_string = os.getenv("AQUA_DB")
    connection = psycopg2.connect(connection_string)

    # Your teardown queries
    delete_version_mutation = queries.delete_bible_version()

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(delete_version_mutation, (revision_id,))
            delete_response = cursor.fetchone()
            delete_check = delete_response[0]

    connection.close()
    return delete_check


# Your test functions here...

if __name__ == "__main__":
    # Example usage
    revision_id = setup_database()
    try:
        # Run your tests here...
        pass
    finally:
        delete_check = teardown_database(revision_id)
