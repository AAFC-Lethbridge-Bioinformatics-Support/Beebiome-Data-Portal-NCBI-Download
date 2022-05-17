import argparse
import logging
import os
from datetime import datetime

import toml

from upload.upload import upload
from download.download_manager import download

config = toml.load("config.toml")
runtime_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
logging.basicConfig(level=config['logging']['level'],
                    format="%(asctime)s [%(levelname)-8s] %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

def main(taxon="Apoidea", filepath=None):
    logger.info("Starting download of {}".format(taxon))
    config["taxon"] = taxon

    if filepath is None:
        filepath = f'./NCBI_xmls_downloads/{taxon}_download_({runtime_timestamp})'

    try:
        os.makedirs(filepath, exist_ok=True)
    except OSError:
        exit(logger.error("Creation of the directory %s failed" % filepath))

    download(filepath, config)

    upload_db = False
    if (taxon == "Apoidea" and upload_db):
        upload(filepath)

    logging.shutdown()
    return filepath


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--filepath", type=str,
                        help="Optional filepath to save the downloaded XMLs to", default=None)
    parser.add_argument(
        "--name", type=str, help="Optional name of taxon subtree to download, default is Apoidea", default="Apoidea")
    args = parser.parse_args()
    main(taxon=args.name, filepath=args.filepath)
