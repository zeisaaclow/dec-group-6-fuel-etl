from elt.connectors.postgresql import PostgreSqlClient
import pytest
from dotenv import load_dotenv
import os
from sqlalchemy import Table, Column, Integer, String, MetaData


# Fixture to set up a PostgreSQL client for testing.
@pytest.fixture
def setup_postgresql_client():

    """
    Pytest fixture to set up a PostgreSQL client for testing.

    This fixture loads environment variables required to connect to the PostgreSQL
    database and initializes a PostgreSqlClient instance.

    Dependencies:
        - The `PostgreSqlClient` class from the `elt.connectors.postgresql` module.
        - Environment variables: TARGET_SERVER_NAME, TARGET_DATABASE_NAME,
          TARGET_DB_USERNAME, TARGET_DB_PASSWORD, TARGET_PORT.

    Returns:
        PostgreSqlClient: An initialized PostgreSQL client instance.

    """

    load_dotenv()
    SERVER_NAME = os.environ.get("TARGET_SERVER_NAME")
    DATABASE_NAME = os.environ.get("TARGET_DATABASE_NAME")
    DB_USERNAME = os.environ.get("TARGET_DB_USERNAME")
    DB_PASSWORD = os.environ.get("TARGET_DB_PASSWORD")
    PORT = os.environ.get("TARGET_PORT")

    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT
    )
    return postgresql_client

@pytest.fixture
def setup_table():
    """
    Pytest fixture to set up a test table for database operations.

    This fixture creates a SQLAlchemy table definition with specified columns
    and returns the table name, table object, and associated metadata.

    Returns:
        Tuple: A tuple containing the table name, table object, and metadata.

    """

    table_name = "test_table"
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("value", String)
    )
    return table_name, table, metadata

def test_insert(setup_postgresql_client, setup_table):
    
    """
    Unit test for inserting and selecting data from a PostgreSQL database table.

    This test demonstrates the functionality of inserting data into a PostgreSQL table,
    selecting all records, and asserting the expected result.

    Dependencies:
        - The `PostgreSqlClient` class from the `elt.connectors.postgresql` module.
        - Environment variables: TARGET_SERVER_NAME, TARGET_DATABASE_NAME,
          TARGET_DB_USERNAME, TARGET_DB_PASSWORD, TARGET_PORT.
        - The `setup_postgresql_client` and `setup_table` fixtures.

    Args:
        setup_postgresql_client (fixture): A fixture providing a PostgreSQL client.
        setup_table (fixture): A fixture providing a test table.

    Test Steps:
        1. Create a PostgreSQL client instance using the fixture.
        2. Create a test table using the fixture.
        3. Insert test data into the table.
        4. Select all records from the table.
        5. Assert that the number of retrieved records matches the expected count.
    """
    postgresql_client = setup_postgresql_client
    table_name, table, metadata = setup_table
    postgresql_client.drop_table(table_name) 

    data = [
        {"id": 1, "value": "hello"},
        {"id": 2, "value": "world"}
    ]

    postgresql_client.insert(data=data, table=table, metadata=metadata)
    
    result = postgresql_client.select_all(table=table)
    assert len(result) == 2

    postgresql_client.drop_table(table_name) 

