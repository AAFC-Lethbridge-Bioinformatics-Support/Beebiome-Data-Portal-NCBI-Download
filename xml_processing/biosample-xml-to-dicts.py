import os
from typing import OrderedDict
import json
import xmltodict
import xml.etree.ElementTree as ET


filepath = "./downloaded-XMLs/run-2022-03-16_15:40:08/biosample/"

def main():
    recordlist = []

    for filename in os.listdir(filepath):
        if filename.endswith(".xml"):
            with open(os.path.join(filepath, filename), "rb") as openfile:
                print("Processing file: " + filename)
                tree = ET.parse(openfile)
                for record in tree.findall(".//BioSample"):
                    cleanrecord = {}
                    parsed = (xmltodict.parse(ET.tostring(record)))['BioSample']

                    cleanrecord['BiosampleAccession'] = parsed.get('@accession')
                    cleanrecord['PublicationDate'] = parsed.get(
                        '@publication_date')
                    cleanrecord['SubmissionDate'] = parsed.get('@submission_date')
                    cleanrecord['UpdateDate'] = parsed.get('@last_update')
                    cleanrecord['Title'] = ((parsed.get(
                        'Description', {})).get("Title"))

                    description = parsed.get('Description', {})
                    cleanrecord['OrganismName'] = description.get("Organism", {}).get("OrganismName")

                    comment = (record.find('Description')).find('Comment')
                    if comment is not None:
                        # cleanrecord['Comment'] = comment
                        cleanrecord['Comment'] = str(ET.tostring((comment))) # temp
                    else:
                        cleanrecord['Comment'] = None

                    attributes = (record.find('Attributes'))
                    cleanrecord['Attributes'] =  str(ET.tostring((attributes)))

                    owner = parsed.get('Owner', {})
                    cleanrecord['Owner'] = owner.get("Name")

                    contact = (owner.get("Contacts", {})).get("Contact")
                    if contact is not None:
                        name = contact.get("Name", {})
                        lastname = name.get("Last")
                        cleanrecord['ContactName'] = name.get("First")
                        if lastname is not None:
                            cleanrecord['ContactName'] += " " + lastname
                    else:
                        cleanrecord['ContactName'] = None

                    ids = parsed['Ids']['Id']
                    for id in ids:
                        if (not isinstance(id, OrderedDict)):
                            cleanrecord['SampleName'] = None
                            cleanrecord['SRA'] = None
                        else:
                            if id.get("@db_label") == 'Sample name':
                                cleanrecord['SampleName'] = id['#text']
                            if id.get("@db") == 'SRA':
                                cleanrecord['SRA'] = id['#text']

                    recordlist.append(cleanrecord)

    with open('biosample-records.json', 'w') as f: # temp
        json.dump(recordlist, f, indent=4)

    return recordlist

if __name__ == "__main__":
    main()