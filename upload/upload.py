
import json
import logging
from datetime import datetime
import os

import mysql.connector
import toml

from processing.bioproject_processing import BioprojectProcessor
from processing.biosample_processing import BiosampleProcessor


config = toml.load("./config.toml")
logger = logging.getLogger(__name__)

def insert_biosamples(filepath, cur):
    for file in os.scandir(filepath):
        records = json.load(open(file))
        print(file)
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
                str(record['Owner']),  # TODO: handle multiple owners case
                record['ContactName'],
                record['SampleName'],
                record['SRA']
            ))

        stmt = ("INSERT INTO Biosample (BiosampleAccession, PublicationDate, SubmissionDate, \
        UpdateDate, Title, OrganismName, Comment, Attributes, Owner, ContactName, SampleName, SRA) \
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        logger.info("Starting executing insert biosample statements...")
        cur.executemany(stmt, values)


def insert_bioprojects(filepath, cur):
    for file in os.scandir(filepath):
        records = json.load(open(file))

        logger.info("Starting executing insert bioproject statements...")
        for record in records:
            try:
                values = (
                    record['BioprojectAccession'],
                    record['Name'],
                    record['Description'],
                    record['ArchiveType'],
                    record['ArchiveID'],
                    record['Submitter'],
                    record['DateSubmitted'],
                    record['DateUpdated'],
                )
            except:
                raise
            stmt = "INSERT INTO Bioproject  (BioprojectAccession, Name, Description, ArchiveType, \
                    ArchiveID, Submitter, DateSubmitted, DateUpdated) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            cur.execute(stmt, values)
            bioproject_id = cur.lastrowid
            print(bioproject_id)
            continue
            publication = record['publication']
            if publication:
                if publication['PublicationDate']:
                    publication['PublicationDate'] = datetime.strptime(publication['PublicationDate'], f"%Y-%m-%dT%H:%M:%S%z").strftime("%Y-%m-%d")
                values = (
                    publication['PublicationDate'],
                    publication['DOI'],
                    publication['Title'],
                    publication['Journal'],
                    publication['Authors'],
                )
                stmt = "INSERT INTO Publication (PublicationDate, DOI, Title, Journal, Authors, BioprojectID) VALUES (%s, %s, %s, %s, %s, LAST_INSERT_ID())"
                cur.execute(stmt, values)

def upload(filepath):
    filepath = "/home/benny/Beebiome-Data-Portal-NCBI-Download/data/Apoidea_(2022-05-26_16-52)_run"


    logger.info("Processing bioproject/biosamples")

    # bioprojects = BioprojectProcessor(filepath + "/bioproject/").run()
    # biosamples = BiosampleProcessor(filepath + "/biosample/").run()


    logger.info("Uploading bioproject/biosamples to %s db...", config["secrets"]["db_host"])
    conn = mysql.connector.connect(
        host=config["secrets"]["db_host"],
        user=config["secrets"]["db_user"],
        password=config["secrets"]["db_pw"],
        database="data"
    )

    cur = conn.cursor()
    cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
    stmt = "TRUNCATE TABLE Bioproject;"
    cur.execute(stmt)
    cur.execute("SET FOREIGN_KEY_CHECKS = 1;")

    bioprojects = "./data/Apoidea_(2022-05-26_16-52)_run/bioproject/jsons/"
    # insert_biosamples(biosamples, cur)
    insert_bioprojects(bioprojects, cur)


    conn.commit()
    cur.close()
    logger.info("Upload to %s db finished", config["secrets"]["db_host"])

if __name__ == "__main__":
    upload("afaf")