import csv
import gzip
import logging
import re
import xml.etree.ElementTree as ET
from typing import List, OrderedDict

import toml
import xmltodict

from XMLDocument import XMLDocument

config = toml.load("./config.toml")
logger = logging.getLogger(__name__)

with open('./upload/processing/blacklists/genera_not_apoidea.txt', 'r') as f:
    genera_not_apoidea = set(f.read().lower().splitlines())

with open('./upload/processing/blacklists/auto-no.csv', 'r') as f:
    reader = csv.reader(f)
    auto_no = [rows[0].casefold() for rows in reader]
    auto_no = "(" + ")|(".join(auto_no) + ")"


class BiosampleDocument(XMLDocument):
    def __init__(self, filepath):
        super().__init__(filepath, "biosample")

    def _parse(self):
        """ Parse the XML file and return a list of records

            Returns:
                A list of records
        """
        record_list = []
        needed_ids = self.ids.copy()

        for _, elm in ET.iterparse(gzip.open(self.filepath, "rb")):
            if elm.tag == "BioSample":
                # elem is record so parse entire element, XML parsing looks messy but works
                clean_record = {}

                record = elm

                parsed = (xmltodict.parse(ET.tostring(record)))[
                    'BioSample']

                if parsed['@id'] not in self.ids:
                    logging.debug("Skipping Biosample record", parsed['@id'])
                    elm.clear()
                    continue
                else:
                    needed_ids.remove(parsed['@id'])

                # Start curation steps

                attributes = parsed.get('Attributes', {}).get('Attribute')
                host = None

                if isinstance(attributes, list):
                    for attribute in attributes:
                        if (attribute.get('@attribute_name') == 'host'):
                            host = attribute.get('#text').casefold()
                            break
                else:
                    if (attributes.get('@attribute_name') == 'host'):
                        host = attributes.get('#text').casefold()

                # skips this iteration if host on blacklists
                if host is None:
                    continue
                if host in genera_not_apoidea:
                    continue
                if re.match(auto_no, host):
                    continue

                # End curation steps

                clean_record['BiosampleAccession'] = parsed.get(
                    '@accession')
                clean_record['PublicationDate'] = parsed.get(
                    '@publication_date')
                clean_record['SubmissionDate'] = parsed.get(
                    '@submission_date')
                clean_record['UpdateDate'] = parsed.get('@last_update')
                clean_record['Title'] = ((parsed.get(
                    'Description', {})).get("Title"))

                description = parsed.get('Description', {})
                clean_record['OrganismName'] = description.get(
                    "Organism", {}).get("OrganismName")
                clean_record['TaxonomyID'] = description.get(
                    "Organism", {}).get("taxonomy_id")

                comment = (record.find('Description')).find('Comment')
                if comment is not None:
                    clean_record['Comment'] = str(ET.tostring((comment)))
                else:
                    clean_record['Comment'] = None

                attributes = (record.find('Attributes'))
                clean_record['Attributes'] = str(ET.tostring((attributes)))

                owner = parsed.get('Owner', {})
                name = owner.get("Name")
                clean_record['Owner'] = None
                if (isinstance(name, OrderedDict)):
                    clean_record['Owner'] = name.get('#text')
                elif (isinstance(name, list)):
                    # TODO: handle multiple names
                    clean_record['Owner'] = name[0]
                elif (isinstance(name, str)):
                    clean_record['Owner'] = name

                if (not isinstance(clean_record['Owner'], str) and clean_record['Owner'] is not None):
                    logger.error(
                        "Owner was not parsed as a string:\n %s ", clean_record["Owner"])
                    raise RuntimeError("Owner was not parsed as a string")

                contact = (owner.get("Contacts", {})).get("Contact")
                if contact is not None:
                    if (isinstance(contact, List)):
                        # TODO: Handle multiple contacts case
                        contact = contact[0]
                    name = contact.get("Name", {})
                    clean_record['ContactName'] = name.get("First")
                    lastname = name.get("Last")
                    if lastname is not None:
                        clean_record['ContactName'] += " " + lastname
                else:
                    clean_record['ContactName'] = None

                ids = parsed['Ids']['Id']
                clean_record['SampleName'] = None
                clean_record['SRA'] = None
                for id in ids:
                    if (not isinstance(id, OrderedDict)):
                        continue
                    else:
                        if id.get("@db_label") == 'Sample name':
                            clean_record['SampleName'] = id['#text']
                        if id.get("@db") == 'SRA':
                            clean_record['SRA'] = id['#text']

                record_list.append(clean_record)
                elm.clear()
            if (len(needed_ids) == 0):
                break

        if (len(needed_ids) > 0):
            logging.warning(
                "Could not find Biosample records for %s", needed_ids)
        return record_list


if __name__ == "__main__":
    BiosampleDocument(
        "./data/Apoidea_(2022-05-24_10-44)_run/biosamples/biosample_set.xml.gz").tojson()
