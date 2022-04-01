# https://entrezpy.readthedocs.io/en/master/tutorials/extending/pubmed.html#putting-everything-together
import entrezpy.conduit
import entrezpy.log.logger
from .taxon_analyzer import TaxonAnalyzer
import logging
import json

logger = logging.getLogger(__name__)

def get_names(email=None, api_key=None, folder="", taxon="Apoidea"):
    ncbi = entrezpy.conduit.Conduit(email, apikey=api_key)

    pipeline = ncbi.new_pipeline()

    taxonomy_ids = pipeline.add_search({'db' : 'taxonomy',
                        'term' : f'{taxon}[subtree]',
                        'rettype' : 'uilist'})

    pipeline.add_fetch({'retmode' : 'xml'}, dependency=taxonomy_ids, analyzer=TaxonAnalyzer())

    res = (ncbi.run(pipeline)).get_result()

    logger.info("Count of names found: " + str(len(res.taxon_names)))
    filepath = f'{folder}/taxon-names.json'
    taxon_names = list(res.taxon_names)
    with open(filepath, 'w') as f:
        json.dump(taxon_names, f)
    return filepath


