### MAIN ###
from jinja2 import Environment, FileSystemLoader
from elt.connectors.postgresql import PostgreSqlClient
from dotenv import load_dotenv
import os 
from elt.assets.extract_load_transform import transform, SqlTransform
#from elt.assets.extract_load_transform import extract_load, transform, SqlTransform
from graphlib import TopologicalSorter
#from elt.assets.pipeline_logging import PipelineLogging
#from elt.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus

if __name__ == "__main__":
    load_dotenv()

    TARGET_DATABASE_NAME = os.environ.get("TARGET_DATABASE_NAME")
    TARGET_SERVER_NAME = os.environ.get("TARGET_SERVER_NAME")
    TARGET_DB_USERNAME = os.environ.get("TARGET_DB_USERNAME")
    TARGET_DB_PASSWORD = os.environ.get("TARGET_DB_PASSWORD")
    TARGET_PORT = os.environ.get("TARGET_PORT")

    target_postgresql_client = PostgreSqlClient(
            server_name=TARGET_SERVER_NAME, 
            database_name=TARGET_DATABASE_NAME,
            username=TARGET_DB_USERNAME,
            password=TARGET_DB_PASSWORD,
            port=TARGET_PORT
        )
    
    transform_template_environment = Environment(loader=FileSystemLoader("elt/assets/sql/transform"))

    # create nodes
    station_prices = SqlTransform(table_name="station_prices", postgresql_client=target_postgresql_client, environment=transform_template_environment)
    brand_fuel_statistics = SqlTransform(table_name="brand_fuel_statistics", postgresql_client=target_postgresql_client, environment=transform_template_environment)
    brand_statistics = SqlTransform(table_name="brand_statistics", postgresql_client=target_postgresql_client, environment=transform_template_environment)

    # create DAG 
    dag = TopologicalSorter()
    dag.add(station_prices)
    dag.add(brand_fuel_statistics, station_prices)
    dag.add(brand_statistics, station_prices)

    transform(dag=dag)