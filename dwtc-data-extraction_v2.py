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

# enterpriseId = "6669a8b54efc9c08dc2ea6c8"
# enterpriseId = "66722edccf46c55b330e493d"

mongo_host = "mongodb+srv://b2cProdUser:evAbj85cFoi1@atlas1.l1978.mongodb.net/B2cProd?retryWrites=true&w=majority"
mongo_dbname = "B2cProd"
mongo_collection = "DictionaryEntity"

# Dev
# esEndPoint = "http://10.10.144.161:9200"

# QA
# esEndPoint = "http://10.30.174.227:9200"

# # Staging
# esEndPoint = "http://172.31.80.92:9200"

# # PROD
esEndPoint = "http://50.0.185.152:9200"

# Connect to MongoDB
mongo_client = MongoClient(mongo_host)
mongo_db = mongo_client[mongo_dbname]
collection = mongo_db[mongo_collection]

# Function to convert ObjectId to string
def convert_object_id(obj_id):
    return str(obj_id) if isinstance(obj_id, ObjectId) else obj_id


# Read CSV file
# csv_file_path = '/Users/siva/Desktop/DWTS/19thJunLoad/Load_1906_DD_Skill_Dictionary.csv'
csv_file_path = '/Users/siva/Desktop/DWTS/19thJunLoad/Load_1906_SKIET_Updated_Skill_Dictionary.csv'
# csv_file_path = '/Users/siva/Desktop/DWTS/19thJunLoad/Test_SKIET_Updated_Skill_Dictionary.csv'
df = pd.read_csv(csv_file_path)

jobs_column = 'jobs'
job_skills_column = 'job_skills'
technical_skills_column = 'technical_skills'
soft_skills_column = 'soft_skills'

# Ensure the CSV columns are correct
required_columns = [
    'Domain', 'Work(EN)', 'Task(EN)',
    'jobs',
    'job_skills',
    'technical_skills',
    'soft_skills'
]
for col in required_columns:
    if col not in df.columns:
        raise ValueError(f"Column '{col}' not found in CSV file")

# Initialize overall counters
total_domain_count = 0
total_work_count = 0
total_task_count = 0
total_skill_count = 0
total_job_count = 0
total_job_skill_count = 0
total_technical_skill_count = 0
total_soft_skill_count = 0

# List to store all documents
all_documents = []
es = Elasticsearch([esEndPoint])

# Process each row in the CSV
# for index, row in df.iterrows():
#     domain_index_key = f"en:{row['Domain'].lower().strip()}"
#     domain_info = mongo_db[mongo_collection].find_one({"IndexKey": domain_index_key})
#
#     if domain_info:
#         domain_id = convert_object_id(domain_info['_id'])
#         domain_title = domain_info['Title']['Title']['en']
#         total_domain_count += 1
#
#         task_index_key = f"en:{row['Task(EN)'].lower().strip()}"
#         task_info = mongo_db[mongo_collection].find_one({"IndexKey": task_index_key, "Type": 25})
#         if task_info:
#             task_document = {
#                 "domain": {
#                     "id": domain_id,
#                     "title": domain_title,
#                     "enterpriseId": enterpriseId
#                 },
#                 "work": {},
#                 "task": {
#                     "id": convert_object_id(task_info['_id']),
#                     "title": task_info['Title']['Title']['en'],
#                     "enterpriseId": enterpriseId,
#                     "definition": task_info['Definition']['Title']['en'] if task_info.get('Definition') else "null"
#                 },
#                 "jobs": [],
#                 "skills": []
#             }
#             total_task_count += 1
#
#             # Fetch work info
#             work_index_key = f"en:{row['Work(EN)'].lower().strip()}"
#             work_info = mongo_db[mongo_collection].find_one({"IndexKey": work_index_key, "Type": 24})
#             if work_info:
#                 task_document['work'] = {
#                     "id": convert_object_id(work_info['_id']),
#                     "title": work_info['Title']['Title']['en'],
#                     "enterpriseId": enterpriseId,
#                     "definition": work_info['Definition']['Title']['en'] if work_info.get('Definition') else "null"
#                 }
#                 total_work_count += 1
#             else:
#                 print(f"No work found for IndexKey: {work_index_key}")
#
#             # Fetch job info
#             job_titles = row[jobs_column].split(', ')
#             for job_title in job_titles:
#                 job_index_key = f"en:{job_title.lower().strip()}"
#                 job_info = mongo_db[mongo_collection].find_one({"IndexKey": job_index_key, "Type": 19})
#                 if job_info:
#                     job_document = {
#                         "id": convert_object_id(job_info['_id']),
#                         "title": job_info['Title']['Title']['en'],
#                         "definition": job_info.get('Definition', {}).get('Title', {}).get('en') if job_info.get('Definition') else "null"
#                     }
#                     task_document['jobs'].append(job_document)
#                     total_job_count += 1
#                 else:
#                     print(f"No job found for IndexKey: {job_index_key}")
#
#             # Fetch skill info (Job Skill)
#             skyhive_job_skill = row[job_skills_column]
#             if isinstance(skyhive_job_skill, float):
#                 skyhive_job_skill = str(skyhive_job_skill)
#             if skyhive_job_skill and skyhive_job_skill.strip():
#                 job_skills = skyhive_job_skill.split('; ')
#                 for skill in job_skills:
#                     skill_index_key = f"en:{skill.lower().strip()}"
#                     skill_info = mongo_db[mongo_collection].find_one({"IndexKey": skill_index_key, "Type": 2})
#                     if skill_info:
#                         skill_document = {
#                             "id": convert_object_id(skill_info['_id']),
#                             "title": skill_info['Title']['Title']['en'],
#                             "definition": skill_info.get('Definition', {}).get('Title', {}).get('en') if skill_info.get('Definition') else "null",
#                             "type": "job"
#                         }
#                         task_document['skills'].append(skill_document)
#                         total_skill_count += 1
#                         total_job_skill_count += 1
#                     else:
#                         print(f"No skill (job) found for IndexKey: {skill_index_key}")
#
#             # Fetch skill info (Technical Skill)
#             skyhive_tech_title = row[technical_skills_column]
#             if isinstance(skyhive_tech_title, float):
#                 skyhive_tech_title = str(skyhive_tech_title)
#             if skyhive_tech_title and skyhive_tech_title.strip():
#                 technical_skills = skyhive_tech_title.split('; ')
#                 for skill in technical_skills:
#                     skill_index_key = f"en:{skill.lower().strip()}"
#                     skill_info = mongo_db[mongo_collection].find_one({"IndexKey": skill_index_key, "Type": 2})
#                     if skill_info:
#                         skill_document = {
#                             "id": convert_object_id(skill_info['_id']),
#                             "title": skill_info['Title']['Title']['en'],
#                             "definition": skill_info.get('Definition', {}).get('Title', {}).get('en') if skill_info.get('Definition') else "null",
#                             "type": "technical"
#                         }
#                         task_document['skills'].append(skill_document)
#                         total_skill_count += 1
#                         total_technical_skill_count += 1
#                     else:
#                         print(f"No skill (technical) found for IndexKey: {skill_index_key}")
#
#             # Fetch skill info (Soft Skill)
#             csv_title = row[soft_skills_column]
#             if isinstance(csv_title, float):
#                 csv_title = str(csv_title)
#             if csv_title and csv_title.strip():
#                 soft_skills = csv_title.split('; ')
#                 for skill in soft_skills:
#                     skill_index_key = f"en:{skill.lower().strip()}"
#                     skill_info = mongo_db[mongo_collection].find_one({"IndexKey": skill_index_key, "Type": 2})
#                     if skill_info:
#                         skill_document = {
#                             "id": convert_object_id(skill_info['_id']),
#                             "title": skill_info['Title']['Title']['en'],
#                             "definition": skill_info.get('Definition', {}).get('Title', {}).get('en') if skill_info.get('Definition') else "null",
#                             "type": "soft"
#                         }
#                         task_document['skills'].append(skill_document)
#                         total_skill_count += 1
#                         total_soft_skill_count += 1
#                     else:
#                         print(f"No skill (soft) found for IndexKey: {skill_index_key}")
#
#             # Index the document into Elasticsearch
#             es.index(index='dwts_v2', document=task_document)
#
#             # Add the document to the list
#             all_documents.append(task_document)
#
#             # Print counts after processing each task
#             print(f"Processed task: {task_info['Title']['Title']['en']}")
#             print(f"Domains inserted so far: {total_domain_count}")
#             print(f"Works inserted so far: {total_work_count}")
#             print(f"Tasks inserted so far: {total_task_count}")
#             print(f"Skills inserted so far: {total_skill_count}")
#             print(f"Jobs inserted so far: {total_job_count}")
#             print(f"Job skills inserted so far: {total_job_skill_count}")
#             print(f"Technical skills inserted so far: {total_technical_skill_count}")
#             print(f"Soft skills inserted so far: {total_soft_skill_count}")
#
#         else:
#             print(f"No task found for IndexKey: {task_index_key}")
#     else:
#         print(f"No domain found for IndexKey: {domain_index_key}")

for index, row in df.iterrows():
    domain_index_key = f"en:{row['Domain'].lower().strip()}"
    domain_info = mongo_db[mongo_collection].find_one({"IndexKey": domain_index_key})

    if domain_info:
        domain_id = convert_object_id(domain_info['_id'])
        domain_title = domain_info['Title']['Title']['en']
        total_domain_count += 1

        task_index_key = f"en:{row['Task(EN)'].lower().strip()}"
        task_info = mongo_db[mongo_collection].find_one({"IndexKey": task_index_key, "Type": 25})
        if task_info:
            task_document = {
                "domain": {
                    "id": domain_id,
                    "title": domain_title
                },
                "work": {},
                "task": {
                    "id": convert_object_id(task_info['_id']),
                    "title": task_info['Title']['Title']['en'],
                    "definition": task_info['Definition']['Title']['en'] if task_info.get('Definition') else "null"
                },
                "jobs": [],
                "skills": []
            }
            total_task_count += 1

            # Fetch work info
            work_index_key = f"en:{row['Work(EN)'].lower().strip()}"
            work_info = mongo_db[mongo_collection].find_one({"IndexKey": work_index_key, "Type": 24})
            if work_info:
                task_document['work'] = {
                    "id": convert_object_id(work_info['_id']),
                    "title": work_info['Title']['Title']['en'],
                    "definition": work_info['Definition']['Title']['en'] if work_info.get('Definition') else "null"
                }
                total_work_count += 1
            else:
                print(f"No work found for IndexKey: {work_index_key}")

            # Fetch job info
            job_titles = row[jobs_column].split(', ')
            for job_title in job_titles:
                job_index_key = f"en:{job_title.lower().strip()}"
                job_info = mongo_db[mongo_collection].find_one({"IndexKey": job_index_key, "Type": 19})
                if job_info:
                    job_document = {
                        "id": convert_object_id(job_info['_id']),
                        "title": job_info['Title']['Title']['en'],
                        "definition": job_info.get('Definition', {}).get('Title', {}).get('en') if job_info.get('Definition') else "null"
                    }
                    task_document['jobs'].append(job_document)
                    total_job_count += 1
                else:
                    print(f"No job found for IndexKey: {job_index_key}")

            # Fetch skill info (Job Skill)
            skyhive_job_skill = row[job_skills_column]
            if isinstance(skyhive_job_skill, float):
                skyhive_job_skill = str(skyhive_job_skill)
            if skyhive_job_skill and skyhive_job_skill.strip():
                job_skills = skyhive_job_skill.split('; ')
                for skill in job_skills:
                    skill_index_key = f"en:{skill.lower().strip()}"
                    skill_info = mongo_db[mongo_collection].find_one({"IndexKey": skill_index_key, "Type": 2})
                    if skill_info:
                        skill_document = {
                            "id": convert_object_id(skill_info['_id']),
                            "title": skill_info['Title']['Title']['en'],
                            "definition": skill_info.get('Definition', {}).get('Title', {}).get('en') if skill_info.get('Definition') else "null",
                            "type": "job"
                        }
                        task_document['skills'].append(skill_document)
                        total_skill_count += 1
                        total_job_skill_count += 1
                    else:
                        print(f"No skill (job) found for IndexKey: {skill_index_key}")

            # Fetch skill info (Technical Skill)
            skyhive_tech_title = row[technical_skills_column]
            if isinstance(skyhive_tech_title, float):
                skyhive_tech_title = str(skyhive_tech_title)
            if skyhive_tech_title and skyhive_tech_title.strip():
                technical_skills = skyhive_tech_title.split('; ')
                for skill in technical_skills:
                    skill_index_key = f"en:{skill.lower().strip()}"
                    skill_info = mongo_db[mongo_collection].find_one({"IndexKey": skill_index_key, "Type": 2})
                    if skill_info:
                        skill_document = {
                            "id": convert_object_id(skill_info['_id']),
                            "title": skill_info['Title']['Title']['en'],
                            "definition": skill_info.get('Definition', {}).get('Title', {}).get('en') if skill_info.get('Definition') else "null",
                            "type": "technical"
                        }
                        task_document['skills'].append(skill_document)
                        total_skill_count += 1
                        total_technical_skill_count += 1
                    else:
                        print(f"No skill (technical) found for IndexKey: {skill_index_key}")

            # Fetch skill info (Soft Skill)
            csv_title = row[soft_skills_column]
            if isinstance(csv_title, float):
                csv_title = str(csv_title)
            if csv_title and csv_title.strip():
                soft_skills = csv_title.split('; ')
                for skill in soft_skills:
                    skill_index_key = f"en:{skill.lower().strip()}"
                    skill_info = mongo_db[mongo_collection].find_one({"IndexKey": skill_index_key, "Type": 2})
                    if skill_info:
                        skill_document = {
                            "id": convert_object_id(skill_info['_id']),
                            "title": skill_info['Title']['Title']['en'],
                            "definition": skill_info.get('Definition', {}).get('Title', {}).get('en') if skill_info.get('Definition') else "null",
                            "type": "soft"
                        }
                        task_document['skills'].append(skill_document)
                        total_skill_count += 1
                        total_soft_skill_count += 1
                    else:
                        print(f"No skill (soft) found for IndexKey: {skill_index_key}")

            # Index the document into Elasticsearch
            es.index(index='dwts', document=task_document)

            # Add the document to the list
            all_documents.append(task_document)

            # Print counts after processing each task
            print(f"Processed task: {task_info['Title']['Title']['en']}")
            print(f"Domains inserted so far: {total_domain_count}")
            print(f"Works inserted so far: {total_work_count}")
            print(f"Tasks inserted so far: {total_task_count}")
            print(f"Skills inserted so far: {total_skill_count}")
            print(f"Jobs inserted so far: {total_job_count}")
            print(f"Job skills inserted so far: {total_job_skill_count}")
            print(f"Technical skills inserted so far: {total_technical_skill_count}")
            print(f"Soft skills inserted so far: {total_soft_skill_count}")

        else:
            print(f"No task found for IndexKey: {task_index_key}")
    else:
        print(f"No domain found for IndexKey: {domain_index_key}")

# Save all documents to a final JSON file
json_output = json.dumps(all_documents, indent=4)
output_file_path = '/Users/siva/Desktop/DWTS/Output/6669a8b54efc9c08dc2ea6c8.json'
with open(output_file_path, 'w') as json_file:
    json_file.write(json_output)

# Final log
print(f"Data has been saved to '{output_file_path}'.")
print(f"Total domains inserted: {total_domain_count}")
print(f"Total works inserted: {total_work_count}")
print(f"Total tasks inserted: {total_task_count}")
print(f"Total skills inserted: {total_skill_count}")
print(f"Total jobs inserted: {total_job_count}")
print(f"Total job skills inserted: {total_job_skill_count}")
print(f"Total technical skills inserted: {total_technical_skill_count}")
print(f"Total soft skills inserted: {total_soft_skill_count}")

print(f"Data loaded successfully")