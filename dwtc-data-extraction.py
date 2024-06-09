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
mongo_host = "mongodb+srv://paas-dev:vrP0gBsRR5wxRGvh@paas-development.irmew.mongodb.net/writeDb?retryWrites=true&w" \
             "=majority"
mongo_dbname = "readDbv1"
mongo_collection = "DictionaryEntity"

# Dev
esEndPoint = "http://10.10.144.161:9200"


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

# Initialize the domain structure
domain_document = {
    "domain": {
        "id": domain_id,
        "title": domain_title,
        "work": []
    }
}

# Fetch work info and construct the work structure
for work_title in df['Work(EN)'].unique():
    work_index_key = f"en:{work_title.lower()}"
    work_info = mongo_db[mongo_collection].find_one({"IndexKey": work_index_key, "Type": 24})
    if work_info:
        work_document = {
            "id": convert_object_id(work_info['_id']),
            "title": work_info['Title']['Title']['en'],
            "definition": work_info['Definition']['Title']['en'] if work_info.get('Definition') else "null",
            "task": []
        }
        domain_document['domain']['work'].append(work_document)
    else:
        print(f"No work found for IndexKey: {work_index_key}")

# Fetch task info and construct the task structure
for index, row in df.iterrows():
    task_index_key = f"en:{row['Task(EN)'].lower()}"
    task_info = mongo_db[mongo_collection].find_one({"IndexKey": task_index_key, "Type": 25})
    if task_info:
        task_document = {
            "id": convert_object_id(task_info['_id']),
            "title": task_info['Title']['Title']['en'],
            "definition": task_info['Definition']['Title']['en'] if task_info.get('Definition') else "null",
            "job": [],
            "skill": []
        }

        # Fetch job info
        job_titles = row['Market Job(from SKYHIVE)'].split(', ')
        for job_title in job_titles:
            job_index_key = f"en:{job_title.lower()}"
            print(f"Fetching job info for index key : {job_index_key}")  # Add this line to print the job title
            job_info = mongo_db[mongo_collection].find_one({"IndexKey": job_index_key, "Type": 19})
            if job_info:
                job_document = {
                    "id": convert_object_id(job_info['_id']),
                    "title": job_info['Title']['Title']['en'],
                    "definition": job_info.get('Definition', {}).get('Title', {}).get('en') if job_info.get(
                        'Definition') else "null"
                }
                task_document['job'].append(job_document)
            else:
                print(f"No job found for IndexKey: {job_index_key}")

        # Fetch skill info (Job Skill)
        skyhive_job_skill = row['Skill (Job Skill, matching from SKYHIVE)']
        if isinstance(skyhive_job_skill, float):
            skyhive_job_skill = str(skyhive_job_skill)
        if skyhive_job_skill and skyhive_job_skill.strip():
            job_skills = skyhive_job_skill.split('; ')
            for skill in job_skills:
                skill_index_key = f"en:{skill.lower()}"
                skill_info = mongo_db[mongo_collection].find_one({"IndexKey": skill_index_key, "Type": 2})
                if skill_info:
                    skill_document = {
                        "id": convert_object_id(skill_info['_id']),
                        "title": skill_info['Title']['Title']['en'],
                        "definition": skill_info.get('Definition', {}).get('Title', {}).get('en') if skill_info.get(
                            'Definition') else "null",
                        "type": "job"
                    }
                    task_document['skill'].append(skill_document)
                else:
                    print(f"No skill (job) found for IndexKey: {skill_index_key}")

        # Fetch skill info (Technical Skill)
        skyhive_tech_title = row['Skill (Technical Skill,matching from SKYHIVE)']
        if isinstance(skyhive_tech_title, float):
            skyhive_tech_title = str(skyhive_tech_title)
        if skyhive_tech_title and skyhive_tech_title.strip():
            technical_skills = skyhive_tech_title.split('; ')
            for skill in technical_skills:
                skill_index_key = f"en:{skill.lower()}"
                skill_info = mongo_db[mongo_collection].find_one({"IndexKey": skill_index_key, "Type": 2})
                if skill_info:
                    skill_document = {
                        "id": convert_object_id(skill_info['_id']),
                        "title": skill_info['Title']['Title']['en'],
                        "definition": skill_info.get('Definition', {}).get('Title', {}).get('en') if skill_info.get(
                            'Definition') else "null",
                        "type": "technical"
                    }
                    task_document['skill'].append(skill_document)
                else:
                    print(f"No skill (technical) found for IndexKey: {skill_index_key}")

        # Fetch skill info (Soft Skill)
        csv_title = row['Skill (Soft Skill, matching from SKYHIVE)']

        # Convert float to string
        if isinstance(csv_title, float):
            csv_title = str(csv_title)
        if csv_title and csv_title.strip():
            soft_skills = csv_title.split('; ')
            for skill in soft_skills:
                skill_index_key = f"en:{skill.lower()}"
                print(f"Fetching skill info for index key : {skill_index_key}")
                skill_info = mongo_db[mongo_collection].find_one({"IndexKey": skill_index_key, "Type": 2})
                if skill_info:
                    skill_document = {
                        "id": convert_object_id(skill_info['_id']),
                        "title": skill_info['Title']['Title']['en'],
                        "definition": skill_info.get('Definition', {}).get('Title', {}).get('en') if skill_info.get(
                            'Definition') else "null",
                        "type": "soft"
                    }
                    task_document['skill'].append(skill_document)
                else:
                    print(f"No skill (soft) found for IndexKey: {skill_index_key}")

        # Find the corresponding work document and add the task
        for work_document in domain_document['domain']['work']:
            if work_document['title'] == row['Work(EN)']:
                work_document['task'].append(task_document)
    else:
        print(f"No task found for IndexKey: {task_index_key}")

es = Elasticsearch([esEndPoint])

if es.ping():
    print("Connected to Elasticsearch.")
else:
    print("Could not connect to Elasticsearch.")

es.index(index='dwts', document=domain_document)

# Output the data in JSON format
json_output = json.dumps(domain_document, indent=4)

# Print the JSON output
print(json_output)

# Define the output file path
output_file_path = '/Users/siva/Desktop/DWTS/output.json'

# Save the JSON output to the specified file
with open(output_file_path, 'w') as json_file:
    json_file.write(json_output)

print(f"Data has been saved to '{output_file_path}'.")
