from datetime import datetime
from names_download.names_download import get_names
from xmls_download.xmls_download import get_xmls
import toml
import entrezpy.log.logger
import argparse
import logging
import os

config = toml.load("config.toml")

entrezpy.log.logger.set_level(config['logging']['level'])
logging.basicConfig(level=config['logging']['level'], format="%(asctime)s [%(levelname)-8s] %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

def main(taxon=None, path=None):

    if (config["secrets"]["api_key"] is None or config["secrets"]["api_key"] == "your-api-key-here"):
        exit(logger.error("No API key provided in config"))
    elif (config["secrets"]["email"] is None or config["secrets"]["email"] == ""):
        logger.warning("No dev contact email provided in config")

    runtime_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")


    if taxon is None:
        taxon = "Apoidea"

    if path is None:
        path = f'./NCBI_xmls_downloads/{taxon}_download_({runtime_timestamp})'

    try:
        os.makedirs(path, exist_ok=True)
    except OSError:
        exit(logger.error("Creation of the directory %s failed" % path))


    get_names(folder=path, taxon=taxon)
    get_xmls(folder=path)

    logging.shutdown()
    exit(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--filepath", type=str, help="Optional filepath to save the downloaded XMLs to")
    parser.add_argument("--name", type=str, help="Optional name of taxon to download, default is Apoidea")
    args = parser.parse_args()
    main(taxon=args.name, path=args.filepath)
