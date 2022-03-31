from datetime import datetime
from taxon_names.get_names import get_names
from xmls.xmls_download import download_xmls
from dotenv import load_dotenv
import entrezpy.log.logger
import argparse
import logging
import os
load_dotenv()

LOGGING_LEVEL = os.getenv('NCBI_DOWNLOAD_LOGGING_LEVEL')
API_KEY = os.getenv('NCBI_API_KEY')
CONTACT_EMAIL = os.getenv('NCBI_API_DEV_CONTACT_EMAIL')

entrezpy.log.logger.set_level(LOGGING_LEVEL)
logging.basicConfig(level=LOGGING_LEVEL, format='%(asctime)s [%(levelname)-8s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

def main(taxon=None, path=None):

    if (API_KEY is None):
        logger.error("No API key provided in .env or enviroment variable")
        exit()
    elif (CONTACT_EMAIL is None):
        logger.warning("No dev contact email provided in .env or enviroment variable")

    runtime_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


    if taxon is None:
        taxon = "Apoidea"

    if path is None:
        path = f'./NCBI-xmls-downloads/{taxon}-{runtime_timestamp}-download'

    try:
        os.makedirs(path, exist_ok=True)
    except OSError:
        logger.error("Creation of the directory %s failed" % path)
    else:
        logger.info("Successfully created the directory %s" % path)

    get_names(email=CONTACT_EMAIL, api_key=API_KEY, folder=path, taxon=taxon)
    download_xmls(email=CONTACT_EMAIL, api_key=API_KEY, folder=path)

    logging.info("Finished downloading XMLs")
    logging.shutdown()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--filepath", type=str, help="Filepath to save xmls")
    parser.add_argument("--name", type=str, help="Name of taxon to download")
    args = parser.parse_args()
    main(taxon=args.name, path=args.filepath)
