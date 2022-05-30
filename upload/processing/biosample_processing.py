import csv
import logging
import os
import re
import xml.etree.ElementTree as ET
from typing import List, OrderedDict

import toml
import xmltodict

from .processing import Processor

config = toml.load("./config.toml")
logger = logging.getLogger(__name__)

with open('./upload/processing/blacklists/genera_not_apoidea.txt', 'r') as f:
    genera_not_apoidea = set(f.read().lower().splitlines())

with open('./upload/processing/blacklists/auto-no.csv', 'r') as f:
    reader = csv.reader(f)
    auto_no = [rows[0].casefold() for rows in reader]
    auto_no = "(" + ")|(".join(auto_no) + ")"

class BiosampleProcessor(Processor):
    def __init__(self, filepath):
        super().__init__(filepath, "biosample")

    @staticmethod
    def _process_file(filepath):
        # XML parsing looks awful but it works
        clean_records = []
        if filepath.endswith(".xml"):
                with open(os.path.join(filepath), "rb") as openfile:
                    tree = ET.parse(openfile)
                    records = tree.findall(".//BioSample")
                    filecount = 0
                    for record in records:

                        filecount += 1
                        clean_record = {}
                        parsed = (xmltodict.parse(ET.tostring(record)))[
                            'BioSample']

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
                        name = owner.get("Name", None)
                        if (isinstance(name, list)):
                            # TODO: handle multiple names
                            name = name[0]

                        if (isinstance(name, OrderedDict) or isinstance(name, dict)):
                            name = name.get('#text')

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

                        clean_records.append(clean_record)
        return clean_records

