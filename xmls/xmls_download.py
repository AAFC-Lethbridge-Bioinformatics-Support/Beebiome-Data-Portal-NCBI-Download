from .export_xml_analyzer import ExportXML
from datetime import datetime
import entrezpy.conduit
import entrezpy.log.logger
import json
import re
import logging

logger = logging.getLogger(__name__)


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

# 514245 uid (probably more) causes Read timeout error by making response too big to read
#  -> fixed by increasing default timeout in entrezpy Requester file

def download_xmls(email=None, api_key=None, folder="."):
    ncbi = entrezpy.conduit.Conduit(email, apikey=api_key)
    queries = make_queries(folder)

    badqueries = []

    for index, query in enumerate(queries):
        logger.info(f'Running Query {index} of {len(queries)}')
        try:
            pipeline = ncbi.new_pipeline()
            biosample_result = pipeline.add_search({'db' : 'biosample', 'term' : query, 'rettype' : 'uilist'})
            pipeline.add_fetch({'retmode':'xml'}, dependency=biosample_result,  analyzer=ExportXML(dbname="biosample", query_num=index, filepath=folder))

            biosample_result = pipeline.add_search({'db' : 'biosample', 'term' : query, 'rettype' : 'uilist'})
            sra_result = pipeline.add_link({'db' : "sra", 'cmd':'neighbor'}, dependency=biosample_result)
            pipeline.add_fetch({'retmode':'xml'}, dependency=sra_result,  analyzer=ExportXML(dbname="sra", query_num=index, filepath=folder))

            biosample_result = pipeline.add_search({'db' : 'biosample', 'term' : query, 'rettype' : 'uilist'})
            bioproject_result = pipeline.add_link({'db' : "bioproject", 'cmd':'neighbor'}, dependency=biosample_result)
            pipeline.add_fetch({'retmode':'xml'}, dependency=bioproject_result,  analyzer=ExportXML(dbname="bioproject", query_num=index, filepath=folder))

            ncbi.run(pipeline)
        except Exception as e:
            template = "An uncaught exception of type {0} occurred. Arguments: {1!r}"
            dump = json.dumps({'exception': template.format(type(e).__name__, e.args)}, indent=4)
            with open(f'{folder}/query-{index}-error-dump.json', "w") as f:
                    f.write(dump)
            logger.error(f'Error in query {index} - uncaught exception: {e}, ignoring & continuing...')
            badqueries.append(index)
            continue

    if (len(badqueries) >= 1):
        logger.error("The following queries (IDs) failed: " + str(badqueries))
    else:
        logger.info("All queries ran successfully")

    return

