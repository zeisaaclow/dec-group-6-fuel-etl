import csv 
import datetime

def transform_datetime(input_datetime):
    parsed_datetime = datetime.datetime.strptime(input_datetime, "%d/%m/%Y %H:%M:%S")
    formatted_datetime = parsed_datetime.strftime("%Y-%m-%d %H:%M:%S")
    return formatted_datetime

def save_to_csv(data_dic, output_name):

    keys = data_dic[0].keys()
    current_time = datetime.datetime.now()
    filename = "elt/data/{}_{}.csv".format(output_name, current_time.strftime('%d%m%Y'))

    
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=keys)

        writer.writeheader()
        
        for row in data_dic:
            if output_name == "prices":
                transformed_datetime = transform_datetime(row["lastupdated"])  # Transform datetime column
                # print(transformed_datetime)
                row["lastupdated"] = transformed_datetime
                writer.writerow(row)
            else:
                writer.writerow(row)




