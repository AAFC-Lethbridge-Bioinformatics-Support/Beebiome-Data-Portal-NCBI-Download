
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
            values = (
                    record['BioprojectAccession'],
                    record['Name'],
                    record['Description'],
                    record['ArchiveType'],
                    record['ArchiveID'],
                    record['SubmissionID'],
                    record['Submitter'],
                    record['DateSubmitted'],
                    record['DateUpdated'],
            )
            stmt = "INSERT INTO Bioproject  (BioprojectAccession, Name, Description, ArchiveType, \
                    ArchiveID, SubmissionID, Submitter, DateSubmitted, DateUpdated) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cur.execute(stmt, values)
            bioproject_id = cur.lastrowid

            resources = record['externallinks']
            for resource in resources:
                values = (
                    resource['Name'],
                    resource['URL'],
                    resource['Category']
                )
                stmt = "INSERT INTO BioProjResource (Name, URL, Category) VALUES (%s, %s, %s)"
                cur.execute(stmt, values)
                resource_id = cur.lastrowid

                stmt = "INSERT INTO BioProjMap (BioprojectID, ResourceID) VALUES (%s, %s)"
                cur.execute(stmt, (bioproject_id, resource_id))


            publications = record['publication']
            for publication in publications:
                values = (
                    publication['ExternalID'],
                    publication['Title'],
                    publication['Journal'],
                    publication['Year'],
                    publication['Volume'],
                    publication['Issue'],
                    publication['PageStart'],
                    publication['PageEnd'],
                    publication['Catalogue'],
                    publication['Status'],
                )
                stmt = "INSERT INTO BioProjPublication (ExternalID, Title, Journal, Year, Volume, Issue, PageStart, PageEnd, Catalogue, Status) \
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cur.execute(stmt, values)
                publication_id = cur.lastrowid

                stmt = "INSERT INTO BioProjPublicationMap (BioprojectID, PublicationID) VALUES (%s, %s)"
                cur.execute(stmt, (bioproject_id, publication_id))

                authors = publication['authors']
                for author in authors:
                    values = (
                        author['FirstName'],
                        author['LastName'],
                        author['Organization']
                    )
                    stmt = "INSERT INTO BioProjPubAuthor (FirstName, LastName, Organization) VALUES (%s, %s, %s)"
                    cur.execute(stmt, values)
                    author_id = cur.lastrowid

                    stmt = "INSERT INTO BioProjPubAuthorMap (BioProjectPublicationID, BioProjectPublicationAuthorID) VALUES (%s, %s)"
                    cur.execute(stmt, (publication_id, author_id))

            prefixes = record['locusprefixes']
            for prefix in prefixes:
                values = (
                    bioproject_id,
                    prefix['LocusTagPrefix'],
                    prefix['BiosampleID'],
                )
                stmt = "INSERT INTO BioProjLTPMap (BioprojectID, LocusTagPrefix, BiosampleID) VALUES (%s, %s, %s)"
                cur.execute(stmt, values)

def upload(filepath):
    filepath = "/home/benny/Beebiome-Data-Portal-NCBI-Download/data/Apoidea_(2022-05-26_16-52)_run"


    logger.info("Processing bioproject/biosamples")

    bioprojects = BioprojectProcessor(filepath + "/bioproject/").run()
    #biosamples = BiosampleProcessor(filepath + "/biosample/").run()


    logger.info("Uploading bioproject/biosamples to %s db...", config["secrets"]["db_host"])
    conn = mysql.connector.connect(
        host=config["secrets"]["db_host"],
        user=config["secrets"]["db_user"],
        password=config["secrets"]["db_pw"],
        database="data"
    )

    cur = conn.cursor()
    cur.execute("SET FOREIGN_KEY_CHECKS = 0")
    cur.execute("TRUNCATE TABLE Bioproject")
    cur.execute("TRUNCATE TABLE BioProjResource")
    cur.execute("TRUNCATE TABLE BioProjPublication")
    cur.execute("TRUNCATE TABLE BioProjPublicationMap")
    cur.execute("TRUNCATE TABLE BioProjPubAuthor")
    cur.execute("TRUNCATE TABLE BioProjPubAuthorMap")
    cur.execute("TRUNCATE TABLE BioProjLTPMap")
    cur.execute("SET FOREIGN_KEY_CHECKS = 1")

    bioprojects = "./data/Apoidea_(2022-05-26_16-52)_run/bioproject/jsons/"
    # insert_biosamples(biosamples, cur)
    insert_bioprojects(bioprojects, cur)


    conn.commit()
    cur.close()
    logger.info("Upload to %s db finished", config["secrets"]["db_host"])

if __name__ == "__main__":
    upload("afaf")