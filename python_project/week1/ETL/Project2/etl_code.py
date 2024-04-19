import glob 
import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET

log_file = "log_file.txt"
target_file = "tranformed_data.csv"

def extract_csv_file(filename):
    dataFrame = pd.read_csv(filename)
    return dataFrame

def extract_json_file(filename):
    dataFrame = pd.read_json(filename,lines = True)
    return dataFrame

def extract_xml_file(filename):
    dataFrame = pd.DataFrame(columns=["car_model", "year_of_manufacture","price","fuel"])
    tree = ET.parse(filename)
    root = tree.getroot()
    for row in root:
        car_model = row.find("car_model").text
        year_of_manufacture = int(row.find("year_of_manufacture").text)
        price = float(row.find("price").text)
        fuel = row.find("fuel").text
        dataFrame = pd.concat([dataFrame,pd.DataFrame([{"car_model":car_model,"year_of_manufacture":year_of_manufacture,"price":price,"fuel":fuel}])], ignore_index=True)
    return dataFrame

def extract_all():
    extracted_data = pd.DataFrame(columns=["car_model", "year_of_manufacture","price","fuel"])
    for csvfile in glob.glob("*.csv"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_csv_file(csvfile))], ignore_index=True)
    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_json_file(jsonfile))], ignore_index=True)
    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_xml_file(xmlfile))], ignore_index=True)
    return extracted_data

def transform_data(extracted_data):
    extracted_data["price"] = extracted_data["price"].round(2)
    return extracted_data

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 
  
# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
  
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract_all() 
  
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
  
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform_data(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
  
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
  
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_data(target_file,transformed_data) 
  
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
  
# Log the completion of the ETL process 
log_progress("ETL Job Ended") 