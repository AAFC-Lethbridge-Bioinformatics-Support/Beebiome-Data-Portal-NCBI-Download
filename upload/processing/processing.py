import multiprocessing
import os
import toml
import logging
config = toml.load("./config.toml")
logger = logging.getLogger(__name__)


class Processor:
    """ Base class to extend for processing data

        Args:
            filepath: Path to the directory to process
            datatype: Type of data being processed (bioproject, biosample or sra)
    """

    def __init__(self, filepath, datatype):
        self.filepath = filepath
        self.datatype = datatype

    def _process_file(filepath):
        """ Process and parse a file """
        raise NotImplementedError

    def run(self):
        """ Parse a entire directory using multiprocessing

            Returns:
                Path to the JSON file with the processed data
        """
        files = [os.path.join(self.filepath, file)
                 for file in (os.listdir(self.filepath))]

        logger.info("Starting processing of %d %s XMLs" %
                    (len(files), self.datatype))

        pool = multiprocessing.Pool()
        pool.imap_unordered(self._process_file, files)
        pool.close()
        pool.join()
        return os.path.join(self.filepath, "jsons")