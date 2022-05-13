
import json
import logging
from datetime import datetime

import mysql.connector
import toml

from .processing.bioproject_processing import BioprojectProcessor
from .processing.biosample_processing import BiosampleProcessor


config = toml.load("./config.toml")
logger = logging.getLogger(__name__)

def insert_biosamples(filepath, cur):
    records = json.load(open(filepath))

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
    records = json.load(open(filepath))

    logger.info("Starting executing insert bioproject statements...")
    for record in records:
        values = (
            record['BioprojectAccession'],
            record['ArchiveID'],
            record['Archive'],
            record['Title'],
            record['Description'],
            record['Relevance'],
            record['Organization'],
            record['DataTypeSet'],
        )
        stmt = "INSERT INTO Bioproject  (BioprojectAccession, ArchiveID, Archive, Title, \
                Description, Relevance, Organization, DataTypeSet) \
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        cur.execute(stmt, values)


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
    logger.info("Processing and uploading bioproject/biosamples to %s db...", config["secrets"]["db_host"])
    conn = mysql.connector.connect(
        host=config["secrets"]["db_host"],
        user=config["secrets"]["db_user"],
        password=config["secrets"]["db_pw"],
        database="data"
    )

    cur = conn.cursor()
    stmt = "TRUNCATE TABLE Biosample; TRUNCATE TABLE Bioproject; TRUNCATE TABLE Publication; TRUNCATE TABLE SRA"
    cur.execute(stmt)

    filepath = "./NCBI_xmls_downloads/Apoidea_download_(2022-05-10_12-51)"

    bioprojects = BioprojectProcessor(filepath + "/bioproject").run()
    biosamples = BiosampleProcessor(filepath + "/biosample").run()
    insert_biosamples(bioprojects, cur)
    insert_bioprojects(biosamples, cur)


    conn.commit()
    cur.close()
    logger.info("Upload to %s db finished", config["secrets"]["db_host"])

