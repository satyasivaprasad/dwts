from bson import ObjectId
from pymongo import MongoClient, UpdateOne
import csv
import pandas as pd
from pymongo import MongoClient

# MongoDB connection parameters
mongo_host = "mongodb+srv://paas-dev:vrP0gBsRR5wxRGvh@paas-development.irmew.mongodb.net/writeDb?retryWrites=true&w" \
             "=majority"
mongo_dbname = "readDbv1"
mongo_collection = "DictionaryEntity"

# Connect to MongoDB
mongo_client = MongoClient(mongo_host)
mongo_db = mongo_client[mongo_dbname]
collection = mongo_db[mongo_collection]

collection.delete_many({"Type": {"$in": [24, 25]}})

# Read the CSV file
csv_file = '/Users/siva/Desktop/DWTS/SKIET_SKill Dictionary_SkyHive요청(20240330).xlsx - 종합 (수정 최종)_mySUNI리뷰.csv'
df = pd.read_csv(csv_file)

print(f"Connected to MongoDB database: {mongo_db.name}")
print(f"CSV file read successfully. Number of rows: {len(df)}")

# Initialize counters
work_count = 0
task_count = 0

# Process and insert Work documents
for index, row in df.iterrows():
    if pd.notna(row['Work(EN)']) and pd.notna(row['Work(KR)']):
        work_doc = {
            "Type": 24,
            "Title": {
                "Title": {
                    "en": row['Work(EN)'],
                    "ko": row['Work(KR)']
                }
            },
            "Definition": None,
            "IndexKey": f"en:{row['Work(EN)'].lower()}"
        }
        collection.insert_one(work_doc)
        work_count += 1
        print(f"Inserted Work document for row {index}: {work_doc}")

# Process and insert Task documents
for index, row in df.iterrows():
    if pd.notna(row['Task(EN)']) and pd.notna(row['Task(KR)']) and pd.notna(row['Task Definition(EN)']) and pd.notna(row['Task Definition(KR)']):
        task_doc = {
            "Type": 25,
            "Title": {
                "Title": {
                    "en": row['Task(EN)'],
                    "ko": row['Task(KR)']
                }
            },
            "Definition": {
                "Title": {
                    "en": row['Task Definition(EN)'],
                    "ko": row['Task Definition(KR)']
                }
            },
            "IndexKey": f"en:{row['Task(EN)'].lower()}"
        }
        collection.insert_one(task_doc)
        task_count += 1
        print(f"Inserted Task document for row {index}: {task_doc}")

print("Data has been inserted successfully.")
print(f"Total Work documents inserted: {work_count}")
print(f"Total Task documents inserted: {task_count}")