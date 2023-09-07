
import csv
from elt.assets.save_csv import save_to_csv
import datetime
from jinja2 import Environment
# from elt.assets.database_extractor import SqlExtractParser, DatabaseTableExtractor
from elt.connectors.postgresql import PostgreSqlClient
from graphlib import TopologicalSorter
import os

#Transformation Class
class SqlTransform:
    def __init__(self, postgresql_client: PostgreSqlClient, environment: Environment, table_name: str):
        self.postgresql_client = postgresql_client
        self.environment = environment
        self.table_name = table_name
        self.template = self.environment.get_template(f"{table_name}.sql")
        
    def create_table_as(self) -> None:
        """
        Drops the table if it exists and creates a new copy of the table using the provided select statement. 
        """
        exec_sql = f"""
            drop table if exists {self.table_name};
            create table {self.table_name} as (
                {self.template.render()}
            )
        """
        self.postgresql_client.execute_sql(exec_sql)

def temp_csv(tables_config, data):
    for table_config in tables_config:
        table_name = table_config["name"]
        table_fuel_price = data[f"{table_name}"]
        if table_fuel_price:
            save_to_csv(data_dic = table_fuel_price, output_name = table_name)

def load_upsert(tables_config, conn):
    for table_config in tables_config:
        table_name = table_config["name"]
        current_time = datetime.datetime.now()

        #check if csv exists
        csv_file_path = "elt/data/{}_{}.csv".format(table_name, current_time.strftime('%d%m%Y'))

        #If file exists perform load/upsert
        if os.path.isfile(csv_file_path):
            cur = conn.cursor()
            
            if table_name == "prices":
                load_columns = "(%s, %s, %s, %s)"
            else:
                load_columns = "(%s, %s, %s, %s, %s, %s, %s)"
            

            # Load CSV data into the table
            with open(csv_file_path, 'r',encoding='utf-8') as csvfile:
                csvreader = csv.reader(csvfile)
                next(csvreader)  # Skip the header row
                truncate_query = f"TRUNCATE TABLE staging_{table_name};"
                cur.execute(truncate_query)
                for row in csvreader:
                    insert_query = f'INSERT INTO staging_{table_name} VALUES {load_columns};'
                    cur.execute(insert_query, row)
                    conn.commit()

            # Perform the upsert using ON CONFLICT

            upsert_query = f"""
                INSERT INTO prices ( stationcode, fueltype , price, lastupdated ) 
                SELECT stationcode, fueltype , price, lastupdated FROM staging_prices
                ON CONFLICT (stationcode, fueltype) DO UPDATE
                SET price = EXCLUDED.price, lastupdated = EXCLUDED.lastupdated;
            """

            cur.execute(upsert_query)

            # Commit changes and close the connection
            conn.commit()

#Transform step: Goes through ordered DAG to build tables for fuel statistics
def transform(dag: TopologicalSorter):
    """
    Performs `create table as` on all nodes in the provided DAG. 
    """
    dag_rendered = tuple(dag.static_order())
    for node in dag_rendered: 
        node.create_table_as()
