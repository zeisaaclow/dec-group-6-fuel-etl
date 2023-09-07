### MAIN ###
from jinja2 import Environment, FileSystemLoader
from elt.connectors.postgresql import PostgreSqlClient
from dotenv import load_dotenv
import os 
from elt.assets.extract_load_transform import transform, SqlTransform
from graphlib import TopologicalSorter
from elt.assets.pipeline_logging import PipelineLogging
from elt.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
from pathlib import Path
import yaml
from elt.connectors.fuel_api import FuelClient
from elt.assets.extract_load_transform import temp_csv, load_upsert
import psycopg2


def run_pipeline(pipeline_config: dict, postgres_logging_client: PostgreSqlClient, fuel_price, tables_config, conn):
    ### CREATE LOGGERS ###
    metadata_logging = MetaDataLogging(pipeline_name=pipeline_config.get("name"), 
                                       postgresql_client=postgres_logging_client, 
                                       config=pipeline_config.get("config"))
    pipeline_logging = PipelineLogging(pipeline_name=pipeline_config.get("name"), 
                                        log_folder_path=pipeline_config.get("config").get("log_folder_path"))
    
    ### INIT TARGET DB CREDENTIALS ###
    TARGET_DATABASE_NAME = os.environ.get("TARGET_DATABASE_NAME")
    TARGET_SERVER_NAME = os.environ.get("TARGET_SERVER_NAME")
    TARGET_DB_USERNAME = os.environ.get("TARGET_DB_USERNAME")
    TARGET_DB_PASSWORD = os.environ.get("TARGET_DB_PASSWORD")
    TARGET_PORT = os.environ.get("TARGET_PORT")
    

    try: 
        metadata_logging.log() # start run
        ### CREATE TARGET CLIENT
        pipeline_logging.logger.info("Creating target client")
        target_postgresql_client = PostgreSqlClient(
            server_name=TARGET_SERVER_NAME, 
            database_name=TARGET_DATABASE_NAME,
            username=TARGET_DB_USERNAME,
            password=TARGET_DB_PASSWORD,
            port=TARGET_PORT
        )
        ### PERFORM EXTRACT AND LOAD ###
        pipeline_logging.logger.info("Perform extract and load")
        temp_csv(data = fuel_price, tables_config = tables_config)
        load_upsert(tables_config=tables_config, conn = conn)

        ### PERFORM TRANSFORM ###
        pipeline_logging.logger.info("Reading transform assets")
        transform_template_environment = Environment(loader=FileSystemLoader("elt/assets/sql/transform"))
        
        # Define Tables to Create
        pipeline_logging.logger.info("Defining Transform Tables")
        station_prices = SqlTransform(table_name="station_prices", postgresql_client=target_postgresql_client, environment=transform_template_environment)
        brand_fuel_statistics = SqlTransform(table_name="brand_fuel_statistics", postgresql_client=target_postgresql_client, environment=transform_template_environment)
        brand_statistics = SqlTransform(table_name="brand_statistics", postgresql_client=target_postgresql_client, environment=transform_template_environment)
        
        # Create DAG 
        pipeline_logging.logger.info("Creating DAG")
        dag = TopologicalSorter()
        dag.add(station_prices)
        dag.add(brand_fuel_statistics, station_prices)
        dag.add(brand_statistics, station_prices)

        # log transform
        pipeline_logging.logger.info("Performing Transform")
        # Run Transformations 
        transform(dag=dag)

        # log completed transformation
        pipeline_logging.logger.info("Completed Pipeline")
        metadata_logging.log(status=MetaDataLoggingStatus.RUN_SUCCESS, logs=pipeline_logging.get_logs()) 
        pipeline_logging.logger.handlers.clear()
    except Exception as e:
        pipeline_logging.logger.error(f"Pipeline failed because of exception: {e}")
        pipeline_logging.logger.handlers.clear()
        

if __name__ == "__main__":
    ### READ LOGGING VARIABLES ####
    load_dotenv()

    API_KEY_ID = os.environ.get("API_KEY")
    AUTHORIZATION = os.environ.get("AUTHORIZATION")

    TARGET_DATABASE_NAME = os.environ.get("TARGET_DATABASE_NAME")
    TARGET_SERVER_NAME = os.environ.get("TARGET_SERVER_NAME")
    TARGET_DB_USERNAME = os.environ.get("TARGET_DB_USERNAME")
    TARGET_DB_PASSWORD = os.environ.get("TARGET_DB_PASSWORD")
    TARGET_PORT = os.environ.get("TARGET_PORT")
    
    LOGGING_SERVER_NAME = os.environ.get("LOGGING_SERVER_NAME")
    LOGGING_DATABASE_NAME = os.environ.get("LOGGING_DATABASE_NAME")
    LOGGING_USERNAME = os.environ.get("LOGGING_USERNAME")
    LOGGING_PASSWORD = os.environ.get("LOGGING_PASSWORD")
    LOGGING_PORT = os.environ.get("LOGGING_PORT")

    postgresql_logging_client = PostgreSqlClient(
        server_name=LOGGING_SERVER_NAME,
        database_name=LOGGING_DATABASE_NAME,
        username=LOGGING_USERNAME,
        password=LOGGING_PASSWORD,
        port=LOGGING_PORT
    )

    
    db_connection_string = f"dbname={TARGET_DATABASE_NAME} user={TARGET_DB_USERNAME} password={TARGET_DB_PASSWORD} host={TARGET_SERVER_NAME} port={TARGET_PORT}"

    # Connect to the database
    conn = psycopg2.connect(db_connection_string)



    fuel_client = FuelClient(api_key_id=API_KEY_ID, authorization=AUTHORIZATION)
    access_token = fuel_client.get_access_token()

    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            multi_pipeline_config = yaml.safe_load(yaml_file)
    else:
        raise Exception(f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name.")

    tables_config = multi_pipeline_config.get("tables")

    extract_type = multi_pipeline_config.get("extract_type")

    fuel_price = fuel_client.get_fuel_api(access_token, extract_type)
    #print(fuel_price)

    ### SCHEDULE PIPELINE ###
    run_pipeline(pipeline_config=multi_pipeline_config, 
                 postgres_logging_client=postgresql_logging_client,
                 fuel_price = fuel_price,
                 tables_config = tables_config,
                 conn = conn)






