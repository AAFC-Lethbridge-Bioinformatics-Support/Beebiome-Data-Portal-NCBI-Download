import json
import logging
import os
import re
from multiprocessing import Process

import enlighten
import entrezpy.conduit

from download.helpers import download_file, get_names
from download.query import Query

logger = logging.getLogger(__name__)
manager = enlighten.get_manager()


def download(filepath, config):
    """ Wrapper for DownloadManager """
    DownloadManager(filepath, config).download()


class DownloadManager:
    def __init__(self, filepath, config):
        """ Manager for downloading XML files and UIDs from NCBI

            Args:
                filepath: Filepath to the folder where the XML files will be downloaded
                config: Config object loaded config.toml
         """
        self.filepath = filepath
        self.config = config
        self.ncbi_connection = self.create_conduit()

    def create_conduit(self):
        """ Create a connection to the NCBI using Entrezpy

            Returns:
                Entrezpy conduit with the configured settings
        """

        config = self.config

        if (config["secrets"]["api_key"] is None or config["secrets"]["api_key"] == "your-api-key-here"):
            logger.error("No API key provided in config")
            raise RuntimeError("No API key provided in config file")
        elif (config["secrets"]["email"] is None or config["secrets"]["email"] == ""):
            logger.warning("No dev contact email provided in config")

        if (config["download"]["use_threads"]):
            conduit = entrezpy.conduit.Conduit(
                config["secrets"]["email"], apikey=config["secrets"]["api_key"], threads=config["download"]["threads"])
        else:
            conduit = entrezpy.conduit.Conduit(
                config["secrets"]["email"], apikey=config["secrets"]["api_key"])
        return conduit

    def download(self):
        """ Starts the download of XML files/UIDs from NCBI

            Uses multiprocessing to reclaim memory/threads (prevent memleak from Requests downloading files) after each query

            (Multiprocessing ran sequentially to not overload our API key quota)

        """
        queries = self.make_queries()

        queries_total = len(queries)
        queries_progress = manager.counter(
            total=queries_total, unit='Query', desc='Queries', leave=False)
        procs = []
        ftp_procs = self.start_ftp_downloads()

        for index, query in enumerate(queries):
            index += 1
            proc = Query(self.ncbi_connection, self.filepath, query, index)
            procs.append(proc)

        for index, proc in enumerate(procs):
            index += 1
            logger.info(f'Running query {index} out of {queries_total}')
            proc.start()
            proc.join()
            if proc.exitcode != 0:
                logger.error(f'Query {index} failed')
                raise RuntimeError("Children process exited unexpectedly")
            proc.close()
            queries_progress.update()

        logger.info("All queries finished")
        logger.info("Checking if FTP downloads finished...")
        for proc in ftp_procs:
            proc.join()
            if proc.exitcode != 0:
                logger.error( f'FTP download {proc.name} exited unexpectedly')
                raise RuntimeError("Children process exited unexpectedly")
            proc.close()
        logging.info("FTP downloads finished")

        queries_progress.close()
        manager.stop()
        logger.info(
            f'Finished running {queries_total} queries. Check folders for any errors.')

        return

    def make_queries(self):
        """ Retrieves a list of names in a given subtree and splits them into queries """
        taxon = self.config["taxon"]
        names_filepath = self.filepath + f'/{taxon}_names.json'

        logger.info("Retrieving names from NCBI")
        proc = Process(target=get_names, args=(
            self.filepath, taxon, self.ncbi_connection))
        proc.start()
        proc.join()
        proc.close()

        with open(names_filepath, 'r') as f:
            names = json.load(f)
        logger.info(str(len(names)) + " names loaded from taxon-names.json")

        queries = []
        first = True
        query = "host[Attribute Name] AND ("

        for name in names:
            # If query longer then ~40k characters - NCBI servers throws internal error, so we split it into multiple queries
            if len(query) >= 20000:
                query += ")"
                queries.append(query)
                query = "host[Attribute Name] AND ("
                first = True
            name = re.sub('[():,./\/]', '', name)
            if first:
                query += "(" + name + " NOT " + name + "[Organism])"
                first = False
            else:
                query += " OR (" + name + " NOT " + name + "[Organism])"
        return queries

    def start_ftp_downloads(self):
        """ Starts the FTP download of Biosample/Bioprojects from NCBI

            Faster to download entire BioProjects/Biosamples at once, then filter them out by UIDs compared to querying API for individual records
        """
        proc1 = Process(name="bioproject-dl", target=download_file, args=(
            "https://ftp.ncbi.nlm.nih.gov/bioproject/bioproject.xml", os.path.join(self.filepath, "bioprojects")))
        proc2 = Process(name="biosample-dl", target=download_file, args=(
            "https://ftp.ncbi.nlm.nih.gov/biosample/biosample_set.xml.gz", os.path.join(self.filepath, "biosamples")))
        proc1.start()
        proc2.start()
        return [proc1, proc2]


