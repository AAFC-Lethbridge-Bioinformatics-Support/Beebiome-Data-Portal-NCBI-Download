# https://entrezpy.readthedocs.io/en/master/tutorials/extending/pubmed.html#putting-everything-together
import entrezpy.conduit
import entrezpy.log.logger
from taxon_analyzer import TaxonAnalyzer
from dotenv import load_dotenv
import logging
import json
import os
load_dotenv()

LOGGING_LEVEL = os.getenv('NCBI_DOWNLOAD_LOGGING_LEVEL')
API_KEY = os.getenv('NCBI_API_KEY')
CONTACT_EMAIL = os.getenv('NCBI_API_DEV_CONTACT_EMAIL')

formatter = logging.Formatter(fmt='%(asctime)s [%(levelname)-8s] %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')

entrezpy.log.logger.set_level(LOGGING_LEVEL)
logger = logging.getLogger(__name__)
logger.setLevel(LOGGING_LEVEL)

def main():
    ncbi = entrezpy.conduit.Conduit(CONTACT_EMAIL, apikey=API_KEY)

    pipeline = ncbi.new_pipeline()

    taxonomy_ids = pipeline.add_search({'db' : 'taxonomy',
                        'term' : 'Apoidea[subtree]',
                        'rettype' : 'uilist'})

    pipeline.add_fetch({'retmode' : 'xml'}, dependency=taxonomy_ids, analyzer=TaxonAnalyzer())

    res = (ncbi.run(pipeline)).get_result()

    logger.info("Count of names found: " + str(len(res.taxon_names)))
    filename = "taxon-names.json"
    taxon_names = list(res.taxon_names)
    with open(filename, 'w') as f:
        json.dump(taxon_names, f)
    return

if __name__ == "__main__":
    main()



