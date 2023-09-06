from elt.connectors.postgresql import PostgreSqlClient
import pytest
from dotenv import load_dotenv
import os
from sqlalchemy import Table, Column, Integer, String, MetaData

@pytest.fixture
def setup_postgresql_client():
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

