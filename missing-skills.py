from bson import ObjectId
from pymongo import MongoClient, UpdateOne
import csv
import pandas as pd
from pymongo import MongoClient
import json
import os
from bson import ObjectId
from elasticsearch import Elasticsearch

# MongoDB connection parameters
# mongo_host = "mongodb+srv://paas-dev:vrP0gBsRR5wxRGvh@paas-development.irmew.mongodb.net/writeDb?retryWrites=true&w" \
#              "=majority"
# mongo_dbname = "readDbv1"
# mongo_collection = "DictionaryEntity"


mongo_host = "mongodb+srv://b2cProdUser:evAbj85cFoi1@atlas1.l1978.mongodb.net/B2cProd?retryWrites=true&w=majority"
mongo_dbname = "B2cProd"
mongo_collection = "DictionaryEntity"


# Connect to MongoDB
mongo_client = MongoClient(mongo_host)
mongo_db = mongo_client[mongo_dbname]
collection = mongo_db[mongo_collection]


# Function to convert ObjectId to string
def convert_object_id(obj_id):
    return str(obj_id) if isinstance(obj_id, ObjectId) else obj_id


# Read CSV file
csv_file_path = '/Users/siva/Desktop/DWTS/SKIET_SKILL_DICTIONARY.csv'
df = pd.read_csv(csv_file_path)
missing_skills_data = []

# Ensure the CSV columns are correct
required_columns = [
    'Domain', 'Work(EN)', 'Task(EN)',
    'Market Job(from SKYHIVE)',
    'Skill (Job Skill, matching from SKYHIVE)',
    'Skill (Technical Skill,matching from SKYHIVE)',
    'Skill (Soft Skill, matching from SKYHIVE)'
]
for col in required_columns:
    if col not in df.columns:
        raise ValueError(f"Column '{col}' not found in CSV file")

# Fetch domain info from MongoDB
domain_index_key = f"en:{df.loc[0, 'Domain'].lower()}"
domain_info = mongo_db[mongo_collection].find_one({"IndexKey": domain_index_key})

if not domain_info:
    raise ValueError(f"No domain found for IndexKey: {domain_index_key}")

domain_id = convert_object_id(domain_info['_id'])
domain_title = domain_info['Title']['Title']['en']

# Process each row in the CSV
# List to store all documents
all_documents = []

# Process each row in the CSV
for index, row in df.iterrows():
    print(f"Processing row {index + 1}/{len(df)}")
    task_index_key = f"en:{row['Task(EN)'].lower()}"
    task_info = mongo_db[mongo_collection].find_one({"IndexKey": task_index_key, "Type": 25})

    if task_info:
        print(f"Task info found for IndexKey: {task_index_key}")
        # Initialize dictionary to store missing skills info
        missing_skills = {
            'IndexKey': task_index_key,
            'Missing Job Skills': '',
            'Missing Technical Skills': '',
            'Missing Soft Skills': ''
        }

        # Check Job Skill
        job_skills = row['Skill (Job Skill, matching from SKYHIVE)']
        if pd.isna(job_skills):
            missing_skills['Missing Job Skills'] = 'No Job Skill provided'
        else:
            for skill in job_skills.split('; '):
                skill_index_key = f"en:{skill.lower()}"
                skill_info = mongo_db[mongo_collection].find_one({"IndexKey": skill_index_key, "Type": 2})
                if not skill_info:
                    missing_skills['Missing Job Skills'] += f"{skill_index_key}"
            if missing_skills['Missing Job Skills']:
                print(f"Missing Job Skills for IndexKey: {task_index_key} - {missing_skills['Missing Job Skills']}")

        # Check Technical Skill
        technical_skills = row['Skill (Technical Skill,matching from SKYHIVE)']
        if pd.isna(technical_skills):
            missing_skills['Missing Technical Skills'] = 'No Technical Skill provided'
        else:
            for skill in technical_skills.split('; '):
                skill_index_key = f"en:{skill.lower()}"
                skill_info = mongo_db[mongo_collection].find_one({"IndexKey": skill_index_key, "Type": 2})
                if not skill_info:
                    missing_skills['Missing Technical Skills'] += f"{skill_index_key}"
            if missing_skills['Missing Technical Skills']:
                print(f"Missing Technical Skills for IndexKey: {task_index_key} - {missing_skills['Missing Technical Skills']}")

        # Check Soft Skill
        soft_skills = row['Skill (Soft Skill, matching from SKYHIVE)']
        if pd.isna(soft_skills):
            missing_skills['Missing Soft Skills'] = 'No Soft Skill provided'
        else:
            for skill in soft_skills.split('; '):
                skill_index_key = f"en:{skill.lower()}"
                skill_info = mongo_db[mongo_collection].find_one({"IndexKey": skill_index_key, "Type": 2})
                if not skill_info:
                    missing_skills['Missing Soft Skills'] += f"{skill_index_key}"
            if missing_skills['Missing Soft Skills']:
                print(f"Missing Soft Skills for IndexKey: {task_index_key} - {missing_skills['Missing Soft Skills']}")

        # Append missing skills to the list
        missing_skills_data.append(missing_skills)
    else:
        print(f"No task found for IndexKey: {task_index_key}")

# Define output CSV file path
output_file_path = '/Users/siva/Desktop/DWTS/output_missing_skills.csv'  # Replace with your output file path

# Write missing skills to new CSV file
with open(output_file_path, mode='w', newline='') as file:
    fieldnames = ['IndexKey', 'Missing Job Skills', 'Missing Technical Skills', 'Missing Soft Skills']
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()
    for data in missing_skills_data:
        writer.writerow(data)

print(f"Missing skills are saved to '{output_file_path}'.")