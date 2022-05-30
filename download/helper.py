import json
import logging
from .queries.save_names.names_analyzer import SaveNames

logger = logging.getLogger(__name__)

# Helper function for download manager

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
