import mysql.connector
from dotenv import load_dotenv
from biosample_processing import biosample_XML_to_dicts
import os
from json import loads, dumps

load_dotenv()

host = os.getenv('BEEBIOME_DB_HOST')
user = os.getenv('BEEBIOME_DB_USER')
pwd = os.getenv('BEEBIOME_DB_PWD')


def to_dict(input_ordered_dict):
    return loads(dumps(input_ordered_dict))


conn = mysql.connector.connect(
    host=host,
    user=user,
    password=pwd,
    database="data"
)
cur = conn.cursor()

#filepath = "./downloaded-XMLs/run-2022-03-17_15:56:24/biosample/"
filepath = "./downloaded-XMLs/run-2022-03-17_14:12:28/biosample"
records = biosample_XML_to_dicts(filepath)

values = []
for record in records:
    values.append((
        record['BiosampleAccession'],
        record['PublicationDate'],
        record['SubmissionDate'],
        record['UpdateDate'],
        record['Title'],
        record['OrganismName'],
        record['Comment'],
        record['Attributes'],
        record['Owner'],
        record['ContactName'],
        record['SampleName'],
        record['SRA']
    ))

stmt = "INSERT INTO biosample (BiosampleAccession, PublicationDate, SubmissionDate, UpdateDate, Title, OrganismName, Comment, Attributes, Owner, ContactName, SampleName, SRA) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
print("Executing insert statements...")
cur.executemany(stmt, values)

conn.commit()
