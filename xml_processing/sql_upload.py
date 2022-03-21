import mysql.connector
import os
from dotenv import load_dotenv
from biosample_processing import biosample_XML_to_dicts
from xml_processing.bioproject_processing import bioproject_XML_to_dicts

load_dotenv()

host = os.getenv('BEEBIOME_DB_HOST')
user = os.getenv('BEEBIOME_DB_USER')
pwd = os.getenv('BEEBIOME_DB_PWD')

def insert_biosamples(filepath, cur):
    biosamplefolder = f'{filepath}biosample/'
    records = biosample_XML_to_dicts(biosamplefolder)

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
    print("Executing insert biosample statements...")
    cur.executemany(stmt, values)

def insert_bioprojects(filepath, cur):
    bioprojectfolder = f'{filepath}bioproject/'
    bioprojectrecords, publicationrecords = bioproject_XML_to_dicts(bioprojectfolder)

    values = []
    for record in bioprojectrecords:
        values.append((
            record['BioprojectAccession'],
            record['PublicationDate'],
            record['SubmissionDate'],
            record['UpdateDate'],
            record['Title'],
            record['Organization'],
            record['DataTypeSet'],
            record['DOI'],
        ))

    stmt = "INSERT INTO bioproject (BioprojectAccession, PublicationDate, SubmissionDate, UpdateDate, Title, Organization, DataTypeSet, DOI) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    print("Executing insert bioproject statements...")
    cur.executemany(stmt, values)

def main():

    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=pwd,
        database="data"
    )
    cur = conn.cursor()

    filepath = "./downloaded-XMLs/run-2022-03-17_14:12:28/"
    # insert_biosamples(filepath, cur)
    insert_bioprojects(filepath, cur)

    conn.commit()
    cur.close()


if __name__ == "__main__":
    main()