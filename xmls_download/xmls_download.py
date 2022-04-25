import imp
from .xmls_analyzer import ExportXML
import entrezpy.conduit
import entrezpy.log.logger
import json
import re
import logging
import toml

config = toml.load("./config.toml")
logger = logging.getLogger(__name__)

downloadBioSample = config["download"]["biosample"]
downloadBioproject = config["download"]["bioproject"]
downloadSRA = config["download"]["sra"]
use_threads = config["download"]["use_threads"]
threads = config["download"]["threads"]

def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]


def create_conduit():
    if (config["download"]["use_threads"]):
        conduit = entrezpy.conduit.Conduit(config["secrets"]["email"], apikey=config["secrets"]["api_key"], threads=config["download"]["threads"])
    else:
        conduit = entrezpy.conduit.Conduit(config["secrets"]["email"], apikey=config["secrets"]["api_key"])
    return conduit

# Make query strings from taxon_names.json
def make_queries(filepath):
    with open(f'{filepath}/taxon-names.json', 'r') as f:
        names = json.load(f)
    logger.info(str(len(names)) + " names loaded from taxon-names.json")

    queries = []
    first = True
    query = "host[Attribute Name] AND ("

    for name in names:
        # if query longer then ~40k characters - NCBI servers throws internal error
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



# Download related data helper function
# Workaround for error thrown from too much UIDs during link operations
def download_related(folder, db, query, query_num):
    ncbi = create_conduit()
    pipeline = ncbi.new_pipeline()
    pipeline.add_search({'db' : 'biosample',
                            'term' : query,
                            'rettype' : 'uilist'})
    analzyer = ncbi.run(pipeline)
    uids = sorted(list(set(analzyer.result.uids)))

    size = 1000
    if len(uids) >= size:
        chunked = list(divide_chunks(uids, size))
    else:
        chunked = [uids]

    totalchunks = len(chunked)
    for index, chunk in enumerate(chunked):
        index += 1
        filenamenum = (str(query_num) + "-" + str(index))
        chunked_uids = list(set(chunk))

        logger.info(f'Running {db} subquery {index} of {totalchunks} for query {query_num}')
        pipeline = ncbi.new_pipeline()
        link_results = pipeline.add_link({'dbfrom':'biosample','db' : db, 'cmd':'neighbor', 'id': chunked_uids, 'link': False})
        pipeline.add_fetch({'retmode':'xml'}, dependency=link_results,  analyzer=ExportXML(dbname=db, query_num=filenamenum, filepath=folder))
        ncbi.run(pipeline)

# Main download function
def get_xmls(folder="."):

    ncbi = create_conduit()
    queries = make_queries(folder)

    totalqueries = len(queries)

    for index, query in enumerate(queries):
        index += 1
        logger.info(f'Running query {index} out of {totalqueries}')

        if downloadBioSample:
            pipeline = ncbi.new_pipeline()
            biosample_result = pipeline.add_search({'db' : 'biosample', 'term' : query, 'rettype' : 'uilist'})
            pipeline.add_fetch({'retmode':'xml'}, dependency=biosample_result,  analyzer=ExportXML(dbname="biosample", query_num=index, filepath=folder))
            ncbi.run(pipeline)

        if downloadSRA:
            download_related(folder, "sra", query, index)
        if downloadBioproject:
            download_related(folder, "bioproject", query, index)

    logger.info(f'Finished running {totalqueries} queries. Check folders for any errors.')

    return

