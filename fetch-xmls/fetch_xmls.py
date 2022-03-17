from export_xml_analyzer import ExportXML
from datetime import datetime
from dotenv import load_dotenv
import entrezpy.conduit
import entrezpy.log.logger
import logging
import json
import os
import re
load_dotenv()

LOGGING_LEVEL = os.getenv('NCBI_DOWNLOAD_LOGGING_LEVEL')
API_KEY = os.getenv('NCBI_API_KEY')
CONTACT_EMAIL = os.getenv('NCBI_API_DEV_CONTACT_EMAIL')

entrezpy.log.logger.set_level(LOGGING_LEVEL)

formatter = logging.Formatter(fmt='%(asctime)s [%(levelname)-8s] %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(LOGGING_LEVEL)

# make queries from taxon_name.json
def make_queries():
    with open('taxon-names.json', 'r') as f:
        names = json.load(f)
    logger.info(str(len(names)) + " names loaded from names.json")

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

def main():
    ncbi = entrezpy.conduit.Conduit(CONTACT_EMAIL, apikey=API_KEY)
    queries = make_queries()

    runtime_timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    badqueries = []

    for index, query in enumerate(queries):
        logger.info("Running Query", index, "of", len(queries))
        try:
            pipeline = ncbi.new_pipeline()
            biosample_result = pipeline.add_search({'db' : 'biosample', 'term' : query, 'rettype' : 'uilist'})
            pipeline.add_fetch({'retmode':'xml'}, dependency=biosample_result,  analyzer=ExportXML("biosample", index, runtime_timestamp))
            sra_result = pipeline.add_link({'db' : "sra", 'cmd':'neighbor'}, dependency=biosample_result)
            pipeline.add_fetch({'retmode':'xml'}, dependency=sra_result,  analyzer=ExportXML("sra", index, runtime_timestamp))
            biosample_result = pipeline.add_search({'db' : 'biosample', 'term' : query, 'rettype' : 'uilist'})
            bioproject_result = pipeline.add_link({'db' : "bioproject", 'cmd':'neighbor'}, dependency=biosample_result)
            pipeline.add_fetch({'retmode':'xml'}, dependency=bioproject_result,  analyzer=ExportXML("bioproject", index, runtime_timestamp))
            ncbi.run(pipeline)
        except Exception as e:
            template = "An uncaught exception of type {0} occurred. Arguments: {1!r}"
            dump = json.dumps({'exception': template.format(type(e).__name__, e.args)}, indent=4)
            with open(f'query-{index}-error-{runtime_timestamp}-run.json', "w") as f:
                    f.write(dump)
            logger.error(f'Error in query {index} - uncaught exception: {e}, ignoring & continuing...')
            badqueries.append(index)
            continue

    if (len(badqueries) >= 1):
        logger.error("The following queries (IDs) failed: " + str(badqueries))
    else:
        logger.info("All queries ran successfully")

    return f'/downloaded-XMLS/run-{runtime_timestamp}/'

if __name__ == "__main__":
    main()

