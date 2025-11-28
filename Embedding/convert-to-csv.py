import json
import csv
import os

input_folder = "DataScience/crawled_company_data"
output_file = "DataScience/Embedding/data/output.csv"

rows = []

for filename in os.listdir(input_folder):
    if filename.endswith(".json"):
        path = os.path.join(input_folder, filename)
        
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                data = [data]

            rows.extend(data)
            

with open(output_file, "w", newline="", encoding="utf-8") as f:
    
    writer = csv.writer(f)
    header = rows[0].keys()
    writer.writerow(header) 
    for item in rows:
      if len(item["Raw_Text"]) < 2000: 
         writer.writerow(item.values())

