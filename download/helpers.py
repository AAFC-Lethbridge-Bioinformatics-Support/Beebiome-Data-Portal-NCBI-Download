import json
import logging
import os
import shutil

import requests

from download.save_names.names_analyzer import SaveNames

logger = logging.getLogger(__name__)

"""
    Helper functions for download manager
"""

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

def download_file(url, filepath):
    local_filename = os.path.join(filepath, url.split('/')[-1])
    with requests.get(url, stream=True) as r:
        r.raw.decode_content = True
        with open(local_filename, 'wb') as f:
            shutil.copyfileobj(r.raw, f,length=16*1024*1024)
    return local_filename
