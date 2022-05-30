import json
import multiprocessing
import os
import sys
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
        self.jsonfile = None

    def _process_file(filepath):
        """ Process and parse a file """
        raise NotImplementedError

    def run(self):
        """ Parse a entire directory using multiprocessing

            Returns:
                Path to the JSON file with the processed data
        """
        record_list = []
        files = [os.path.join(self.filepath, file)
                 for file in (os.listdir(self.filepath))]

        logger.info("Starting processing of %d %s XMLs" %
                    (len(files), self.datatype))

        pool = multiprocessing.Pool()
        results = pool.map(self._process_file, files)
        pool.close()
        pool.join()

        for result in results:
            record_list.extend(result)

        totalcount = len(record_list)
        if (totalcount == 0):
            sys.exit(logger.error("No records found in %s" % (self.filepath)))

        print("# total %s record processed: %d" % (self.datatype, totalcount))

        # remove dupe records
        record_list = {json.dumps(d, sort_keys=True) for d in record_list}
        record_list = [json.loads(t) for t in record_list]

        logger.info("# total unique %s records processed: %d"
                    % (self.datatype, len(record_list)))

        savelocation = os.path.join(
            os.path.dirname(self.filepath), "processed/")
        os.makedirs(savelocation, exist_ok=True)
        savefile = os.path.join(
            savelocation, ("%s_records.json" % self.datatype))
        with open(savefile, 'w') as f:
            json.dump(record_list, f, indent=4)
        self.jsonfile = savelocation
        return savelocation
