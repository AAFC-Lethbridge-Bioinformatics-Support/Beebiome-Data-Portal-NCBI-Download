import json
import logging
import re
from multiprocessing import Process

import enlighten
import entrezpy.conduit

from download.queries import get_names, run_one_query

logger = logging.getLogger(__name__)
manager = enlighten.get_manager()

def download(filepath, config):
    """ Wrapper for DownloadManager """
    DownloadManager(filepath, config).download()

class DownloadManager:
    def __init__(self, filepath, config):
        """ Manager for downloading XML files from NCBI

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
            exit(logger.error("No API key provided in config"))
        elif (config["secrets"]["email"] is None or config["secrets"]["email"] == ""):
            logger.warning("No dev contact email provided in config")

        if (config["download"]["use_threads"]):
            conduit = entrezpy.conduit.Conduit(
                config["secrets"]["email"], apikey=config["secrets"]["api_key"], threads=config["download"]["threads"])
        else:
            conduit = entrezpy.conduit.Conduit(
                config["secrets"]["email"], apikey=config["secrets"]["api_key"])
        return conduit

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

    def download(self):
        """ Starts the download of XML files from NCBI """

        queries = self.make_queries()

        queries_total = len(queries)
        queries_progress = manager.counter(
            total=queries_total, unit='Query', desc='Queries', leave=False)
        procs = []

        for index, query in enumerate(queries):
            # https://stackoverflow.com/questions/14270053/python-requests-not-clearing-memory-when-downloading-with-sessions
            # Spawn a child process to reclaim the memory after each query (or else memory leak)
            index += 1
            proc = Process(target=run_one_query, args=(
                self.filepath, query, index, self.config, self.ncbi_connection))
            procs.append(proc)

        # Running child processes sequentially to not overload our API key quota
        for index, proc in enumerate(procs):
            index += 1
            logger.info(f'Running query {index} out of {queries_total}')
            proc.start()
            proc.join()
            proc.close()
            queries_progress.update()

        queries_progress.close()
        manager.stop()
        logger.info(
            f'Finished running {queries_total} queries. Check folders for any errors.')

        return
