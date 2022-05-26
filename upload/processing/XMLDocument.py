import json
import logging
import os
import sys
import xml.etree.ElementTree as ET
import toml

config = toml.load("./config.toml")
logger = logging.getLogger(__name__)


class XMLDocument:
    """ Base class to store XML file

        Args:
            filepath: Path to the file to process
            datatype: Type of data being processed (bioproject, biosample or sra)
    """

    def __init__(self, filepath, datatype):

        self.filepath = filepath
        self.datatype = datatype
        self.jsonfile = None
        self.ids = None
        self.setUIDs()

    def setUIDs(self, filepath=None):
        """ Set the UIDs to filter for when parsing the XML file.\n
            Default behaviour looks for JSON file to set UIDs in the same directory as specified filepath

            Args:
                filepath: Path to the JSON file with the UIDs (optional)
        """
        if (filepath):
            self.ids = json.load(open(filepath, "r"))
        else:
            self.ids = json.load(open(os.path.join(
                os.path.dirname(self.filepath), ("%s_UIDs.json" % self.datatype)), "r"))

    def _parse(self):
        """ Parse the XML file and return a list of records

            Returns:
                A list of records
        """
        raise NotImplementedError

    def tojson(self):
        """ Return a JSON representation of the XML file, filtered by the UIDs

            Returns:
                Path to the JSON file with the processed data
        """
        record_list = self._parse()
        totalcount = len(record_list)
        if (totalcount == 0):
            sys.exit(logger.error("No records found in %s" % (self.filepath)))

        logger.debug("# total %s record processed: %d" % (self.datatype, totalcount))

        # remove dupe records
        record_list = {json.dumps(d, sort_keys=True) for d in record_list}
        record_list = [json.loads(t) for t in record_list]

        logger.info("# total unique %s records processed: %d"
                    % (self.datatype, len(record_list)))

        savefile = os.path.join(
            os.path.dirname(self.filepath), ("%s_records.json" % self.datatype))
        with open(savefile, 'w') as f:
            json.dump(record_list, f, indent=4)
        self.jsonfile = savefile
        return savefile

