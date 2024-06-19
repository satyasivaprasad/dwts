from bson import ObjectId
from pymongo import MongoClient, UpdateOne
import csv
import pandas as pd
from pymongo import MongoClient

# MongoDB connection parameters
# mongo_host = "mongodb+srv://paas-dev:vrP0gBsRR5wxRGvh@paas-development.irmew.mongodb.net/writeDb?retryWrites=true&w" \
#              "=majority"
# mongo_dbname = "readDbv1"
# mongo_collection = "DictionaryEntity"

#QA
# mongo_host = "mongodb+srv://b2cDictionaryReadOnly:8Yeoc9kQZJ0z8J66@atlas1-l1978.mongodb.net/B2cProd?retryWrites=true&w=majority"
# mongo_dbname = "B2cProd"
# mongo_collection = "DictionaryEntity"

#PROD
mongo_host = "mongodb+srv://b2cProdUser:evAbj85cFoi1@atlas1.l1978.mongodb.net/B2cProd?retryWrites=true&w=majority"
mongo_dbname = "B2cProd"
mongo_collection = "DictionaryEntity"

# Connect to MongoDB
mongo_client = MongoClient(mongo_host)
mongo_db = mongo_client[mongo_dbname]
collection = mongo_db[mongo_collection]

collection.delete_many({"Type": {"$in": [23, 24, 25]}})

# Read the CSV file
# csv_file = '/Users/siva/Desktop/DWTS/SKIET_SKill Dictionary_SkyHive요청(20240330).xlsx - 종합 (수정 최종)_mySUNI리뷰.csv'
csv_file = '/Users/siva/Desktop/DWTS/19thJunLoad/19_06_DWTS_Data.csv'
df = pd.read_csv(csv_file)

print(f"Connected to MongoDB database: {mongo_db.name}")
print(f"CSV file read successfully. Number of rows: {len(df)}")

# Initialize counters
domain_count = 0
work_count = 0
task_count = 0

# Initialize sets to track unique values
unique_domains = set()
unique_works = set()
unique_tasks = set()

# Process and insert Domain documents
for index, row in df.iterrows():
    if pd.notna(row['Domain']):
        domain_value = row['Domain'].strip()  # Trim the domain value
        if domain_value.lower() not in unique_domains:  # Check for uniqueness
            unique_domains.add(domain_value.lower())  # Add to the set of unique values
            domain_doc = {
                "Type": 23,
                "Title": {
                    "Title": {
                        "en": domain_value,
                        "ko": ''
                    }
                },
                "Definition": None,
                "IndexKey": f"en:{domain_value.lower()}"
            }
            collection.insert_one(domain_doc)
            domain_count += 1
            print(f"Inserted Domain document for row {index}: {domain_doc}")

# Process and insert Work documents
for index, row in df.iterrows():
    if pd.notna(row['Work(EN)']):
        work_en_value = row['Work(EN)'].strip()  # Trim the Work(EN) value
        if work_en_value.lower() not in unique_works:  # Check for uniqueness
            unique_works.add(work_en_value.lower())  # Add to the set of unique values
            work_doc = {
                "Type": 24,
                "Title": {
                    "Title": {
                        "en": work_en_value,
                        "ko": ''
                    }
                },
                "Definition": None,
                "IndexKey": f"en:{work_en_value.lower()}"
            }
            collection.insert_one(work_doc)
            work_count += 1
            print(f"Inserted Work document for row {index}: {work_doc}")

# Process and insert Task documents
for index, row in df.iterrows():
    if pd.notna(row['Task(EN)']) and pd.notna(row['Task(KR)']) and pd.notna(row['Task Definition(EN)']) and pd.notna(row['Task Definition(KR)']):
        task_en_value = row['Task(EN)'].strip()  # Trim the Task(EN) value
        if task_en_value.lower() not in unique_tasks:  # Check for uniqueness
            unique_tasks.add(task_en_value.lower())  # Add to the set of unique values
            task_kr_value = row['Task(KR)'].strip()  # Trim the Task(KR) value
            task_def_en_value = row['Task Definition(EN)'].strip()  # Trim the Task Definition(EN) value
            task_def_kr_value = row['Task Definition(KR)'].strip()  # Trim the Task Definition(KR) value
            task_doc = {
                "Type": 25,
                "Title": {
                    "Title": {
                        "en": task_en_value,
                        "ko": task_kr_value
                    }
                },
                "Definition": {
                    "Title": {
                        "en": task_def_en_value,
                        "ko": task_def_kr_value
                    }
                },
                "IndexKey": f"en:{task_en_value.lower()}"
            }
            collection.insert_one(task_doc)
            task_count += 1
            print(f"Inserted Task document for row {index}: {task_doc}")

print("Data has been inserted successfully.")
print(f"Total Domain documents inserted: {domain_count}")
print(f"Total Work documents inserted: {work_count}")
print(f"Total Task documents inserted: {task_count}")