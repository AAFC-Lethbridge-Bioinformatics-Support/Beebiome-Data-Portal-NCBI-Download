# https://entrezpy.readthedocs.io/en/master/tutorials/extending/pubmed.html#putting-everything-together
import entrezpy.conduit
import entrezpy.log.logger
from .names_analyzer import NamesAnalyzer
import logging
import json
import toml

config = toml.load("./config.toml")
logger = logging.getLogger(__name__)

def get_names(folder="", taxon="Apoidea"):
    if config['download']['use_threads']:
        ncbi = entrezpy.conduit.Conduit(config["secrets"]["email"], apikey=config["secrets"]["api_key"], threads=config["download"]["threads"])
    else:
        ncbi = entrezpy.conduit.Conduit(config["secrets"]["email"], apikey=config["secrets"]["api_key"])

    pipeline = ncbi.new_pipeline()

    taxonomy_ids = pipeline.add_search({'db' : 'taxonomy',
                        'term' : f'{taxon}[subtree]',
                        'rettype' : 'uilist'})

    pipeline.add_fetch({'retmode' : 'xml'}, dependency=taxonomy_ids, analyzer=NamesAnalyzer())

    res = (ncbi.run(pipeline)).get_result()

    logger.info("Count of names found: " + str(len(res.taxon_names)))
    taxon_names = list(res.taxon_names)

    filepath = f'{folder}/taxon-names.json'
    with open(filepath, 'w') as f:
        json.dump(taxon_names, f)
    return filepath


