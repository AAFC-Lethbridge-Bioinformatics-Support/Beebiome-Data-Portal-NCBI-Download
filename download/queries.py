import json
import logging
from multiprocessing import Process

from download.save_names.names_analyzer import SaveNames
from download.save_xmls.xml_analyzer import SaveXMLs

logger = logging.getLogger(__name__)

# TODO: Find a better code structure for this

def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]

def get_names(filepath, taxon, ncbi_connection):
    pipeline = ncbi_connection.new_pipeline()

    taxonomy_ids = pipeline.add_search({'db' : 'taxonomy',
                        'term' : f'{taxon}[subtree]',
                        'rettype' : 'uilist'})

    pipeline.add_fetch({'retmode' : 'xml'}, dependency=taxonomy_ids, analyzer=SaveNames())

    res = (ncbi_connection.run(pipeline)).get_result()

    logger.info("Count of names found: " + str(len(res.taxon_names)))
    taxon_names = list(res.taxon_names)

    filepath = f'{filepath}/{taxon}_names.json'
    with open(filepath, 'w') as f:
        json.dump(taxon_names, f)
    return filepath

def run_one_query(filepath, query, query_num, config, ncbi_connection):
    """ Run a single query for results and then queries for linked data in other dbs

        Given query ran on the biosample db first, and then uses the results to find related bioprojects and sra files.
        XML files are downloaded to the filepath given as part of the process.
    """
    proc = Process(target=run_one_query_biosamples, args=(
            filepath, query, query_num, ncbi_connection))
    proc.start()
    proc.join()
    proc.close()
    if config["download"]["sra"]:
        run_related_queries(filepath, "sra", query, query_num, ncbi_connection)
    if config["download"]["bioproject"]:
        run_related_queries(filepath, "bioproject",
                            query, query_num, ncbi_connection)
    return

def run_one_query_biosamples(filepath, query, query_num, ncbi_connection):
    pipeline = ncbi_connection.new_pipeline()
    biosample_result = pipeline.add_search(
        {'db': 'biosample', 'term': query, 'rettype': 'uilist'})
    pipeline.add_fetch({'retmode': 'xml'}, dependency=biosample_result,  analyzer=SaveXMLs(
        dbname="biosample", query_num=query_num, filepath=filepath))
    ncbi_connection.run(pipeline)
    return

def run_related_queries(filepath, db, query, query_num, ncbi_connection):
    """ Find and downloads linked data (bioproject and sra)

        Entrezpy linking not working properly for large amount of biosample IDs
        so IDs are retreived again and split into chunks of 1000
        for indivdual link requests, and are ran as child processes
        for memory/thread management
    """
    pipeline = ncbi_connection.new_pipeline()
    pipeline.add_search({'db': 'biosample',
                         'term': query,
                         'rettype': 'uilist'})
    analzyer = ncbi_connection.run(pipeline)
    uids = sorted(list(set(analzyer.result.uids))) # TODO: Save the IDs from the last biosample search?

    size = 1000
    if len(uids) >= size:
        chunked = list(divide_chunks(uids, size))
    else:
        chunked = [uids]

    total_chunks = len(chunked)
    procs = []

    for index, chunk in enumerate(chunked):
        index += 1
        filenamenum = (str(query_num) + "-" + str(index))
        chunk = list(set(chunk))
        proc = Process(target=run_one_related_query, args=(
            filepath, db, filenamenum, chunk, ncbi_connection))
        procs.append(proc)

    for index, proc in enumerate(procs):
        index += 1
        logger.debug(
            f'Running {db} subquery {index} of {total_chunks} for query {query_num}')
        proc.start()
        proc.join()
        proc.close()
    return


def run_one_related_query(filepath, db, filenamenum, ids, ncbi_connection):
    pipeline = ncbi_connection.new_pipeline()
    link_results = pipeline.add_link(
        {'dbfrom': 'biosample', 'db': db, 'cmd': 'neighbor', 'id': ids, 'link': False})
    pipeline.add_fetch({'retmode': 'xml'}, dependency=link_results,  analyzer=SaveXMLs(
        dbname=db, query_num=filenamenum, filepath=filepath))
    ncbi_connection.run(pipeline)
