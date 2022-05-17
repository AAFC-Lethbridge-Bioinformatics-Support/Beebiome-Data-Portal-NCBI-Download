import os
import shutil
import subprocess
import logging
from main import main as main_script

logger = logging.getLogger(__name__)

"""
Wrapper script for running in GCP.
"""

download_folder = "./NCBI_xmls_downloads"
try:
    main_script()

    # Compressed downloaded files
    files = [os.path.join(download_folder, file)
             for file in os.listdir(download_folder)]
    for file in files:
        if os.path.isdir(file):
            shutil.make_archive(file, 'zip', file)
            subprocess.run(["rm", "-rf", os.path.join(file)])

    # Remove oldest run
    files = [os.path.join(download_folder, file)
             for file in os.listdir(download_folder)]
    if len(files) >= 3:
        oldest_file = min(files, key=os.path.getmtime)
        subprocess.run(["rm", "-rf", oldest_file])

    # Remove oldest log file
    files = [os.path.join("./logs/", file) for file in os.listdir("./logs/")]
    if len(files) >= 6:
        oldest_file = min(files, key=os.path.getmtime)
        subprocess.run(["rm", oldest_file])

except Exception as e:
    logger.critical(e, exc_info=True)  # failsafe

# stop the machine script is running on
subprocess.call(["shutdown"])
