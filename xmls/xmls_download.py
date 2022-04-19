import traceback
from .export_xml_analyzer import ExportXML
import entrezpy.conduit
import entrezpy.log.logger
import json
import re
import logging

logger = logging.getLogger(__name__)

def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]

# make queries from taxon_name.json
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


# workaround, doing it all in one conduit using entrezpy resulted in too many uids for one link operation (missing records) so chunk it
def download_related(config, db, query, query_num):
    ncbi = entrezpy.conduit.Conduit(config["email"], config["api_key"])
    pipeline = ncbi.new_pipeline()
    pipeline.add_search({'db' : 'biosample',
                            'term' : query,
                            'rettype' : 'uilist'})
    analzyer = ncbi.run(pipeline)
    uids = sorted(list(set(analzyer.result.uids)))


    # if number too big, internal server error code 500 from ncbi
    size = 1000

    if len(uids) >= size:
        chunked = list(divide_chunks(uids, size))
    else:
        chunked = [uids]

    totalchunks = len(chunked)
    for index, chunk in enumerate(chunked):
        filenamenum = (str(query_num) + "-" + str(index))
        chunked_uids = list(set(chunk))
        index += 1
        logger.info(f'Running {db} subquery {index} of {totalchunks} for query {query_num}')
        try:
            pipeline = ncbi.new_pipeline()
            link_results = pipeline.add_link({'dbfrom':'biosample','db' : db, 'cmd':'neighbor', 'id': chunked_uids, 'link': False})
            pipeline.add_fetch({'retmode':'xml'}, dependency=link_results,  analyzer=ExportXML(dbname=db, query_num=filenamenum, filepath=config["folder"]))
            ncbi.run(pipeline)
        except RuntimeError as e:
            logger.error(f'Unsucessful {db} subquery {index} of {totalchunks} for query {query_num}')
            pass
        except Exception as e:
            dump = json.dumps({'func':__name__, 'uids': str(chunked_uids),'exeception': str(e), 'traceback': traceback.format_exc()}, indent=4)
            with open(f'{config["folder"]}/query-{filenamenum}-{db}-error.log', "w") as f:
                    f.write(dump)
            logger.error(f'Exception when running {db} subquery {index} for {query_num}; see logs in {config["folder"]} folder.')
            pass

def download_xmls(email=None, api_key=None, folder=".",  downloadBioproject = True, downloadSRA = True, downloadBioSample = True):
    config = {
        "email": email,
        "api_key": api_key,
        "folder": folder
    }
    ncbi = entrezpy.conduit.Conduit(email, apikey=api_key)
    queries = make_queries(folder)

    totalqueries = len(queries)
    queries.reverse() # fail fast
    for index, query in enumerate(queries):
        index += 1
        logger.info(f'Running query {index} out of {totalqueries}')
        if downloadBioSample:
            pipeline = ncbi.new_pipeline()
            biosample_result = pipeline.add_search({'db' : 'biosample', 'term' : query, 'rettype' : 'uilist'})
            pipeline.add_fetch({'retmode':'xml'}, dependency=biosample_result,  analyzer=ExportXML(dbname="biosample", query_num=index, filepath=folder))
            ncbi.run(pipeline)

        if downloadSRA:
            download_related(config, "sra", query, index)
        if downloadBioproject:
            download_related(config, "bioproject", query, index)

    logger.info(f'Finished running {totalqueries} queries. Check folders for any errors.')

    return

