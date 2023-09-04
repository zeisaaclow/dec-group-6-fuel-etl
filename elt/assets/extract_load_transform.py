
import csv
from assets.save_csv import save_to_csv
import datetime



def temp_csv(data, tables_config):
    for table_config in tables_config:
        table_name = table_config["name"]
        table_fuel_price = data[f"{table_name}"]
        save_to_csv(data_dic = table_fuel_price, output_name = table_name)


# Connect to PostgreSQL
def load_upsert(conn,tables_config):
    for table_config in tables_config:
        table_name = table_config["name"]
        current_time = datetime.datetime.now()

        cur = conn.cursor()
        
        if table_name == "prices":
            load_columns = "(%s, %s, %s, %s)"
        else:
            load_columns = "(%s, %s, %s, %s, %s, %s, %s)"
        # Path to your CSV file
        csv_file_path = "data/{}_{}.csv".format(table_name, current_time.strftime('%d%m%Y'))

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
            # upsert_query = f"""
            #     INSERT INTO stations ( brandid,stationid,brand,code,name,address,location ) 
            #     SELECT brandid,stationid,brand,code,name,address,location FROM staging_station
            #     ON CONFLICT (code) DO UPDATE
            #     SET brandid = EXCLUDED.brandid, stationid = EXCLUDED.stationid, brand = EXCLUDED.brand, name = EXCLUDED.name, address = EXCLUDED.address,  location = EXCLUDED.location;
            # """

        cur.execute(upsert_query)

        # Commit changes and close the connection
        conn.commit()


