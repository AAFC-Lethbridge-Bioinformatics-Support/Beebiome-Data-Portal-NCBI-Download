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

    bioprojects = []
    for record in bioprojectrecords:
        bioprojects.append((
            record['BioprojectAccession'],
            record['ArchiveID'],
            record['Archive'],
            record['Title'],
            record['Description'],
            record['Relevance'],
            record['Organization'],
            record['DataTypeSet'],
        ))
    stmt = "INSERT INTO bioproject (BioprojectAccession, ArchiveID, Archive, Title, Description, Relevance, Organization, DataTypeSet) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    print("Executing insert bioproject statements...")
    cur.executemany(stmt, bioprojects)

    publications = []
    for record in publicationrecords:
        publications.append((
            record['PublicationDate'],
            record['Bioproject'],
            record['DOI'],
            record['Title'],
            record['Journal'],
            record['Authors'],
        ))

    for record in publications:
        # ?
        stmt = "INSERT INTO publication (PublicationDate, BioProjectID, DOI, Title, Journal, Authors) VALUES (%s, (SELECT ID FROM bioproject WHERE ), %s, %s, %s, %s)"
        print("Executing insert publication statement...")
        cur.executemany(stmt, record)



def main():

    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=pwd,
        database="data"
    )
    cur = conn.cursor()

    filepath = "./downloaded-XMLs/run-2022-03-22_14:10:06/"
    # insert_biosamples(filepath, cur)
    insert_bioprojects(filepath, cur)

    conn.commit()
    cur.close()


if __name__ == "__main__":
    main()