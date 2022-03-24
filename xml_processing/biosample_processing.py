import os
from typing import List, OrderedDict
import json
import xmltodict
import xml.etree.ElementTree as ET
import argparse

def biosample_XML_to_dicts(filepath):
    recordlist = []
    count = 0
    files = os.listdir(filepath)
    for i, filename in enumerate(files):
        if filename.endswith(".xml"):

            with open(os.path.join(filepath, filename), "rb") as openfile:
                print("Processing file " + str(i) + "/" + str(len(files)) + ": " + filename)
                tree = ET.parse(openfile)
                for record in tree.findall(".//BioSample"):
                    count += 1
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
                        cleanrecord['Comment'] = str(ET.tostring((comment)))
                    else:
                        cleanrecord['Comment'] = None

                    attributes = (record.find('Attributes'))
                    cleanrecord['Attributes'] =  str(ET.tostring((attributes)))

                    owner = parsed.get('Owner', {})
                    name = owner.get("Name")
                    if (isinstance(name, OrderedDict)):
                        cleanrecord['Owner'] = name.get('#text')
                    else:
                        cleanrecord['Owner'] = name

                    contact = (owner.get("Contacts", {})).get("Contact")
                    if contact is not None:
                        if (isinstance(contact, List)):
                            contact = contact[0]
                        name = contact.get("Name", {})
                        lastname = name.get("Last")
                        cleanrecord['ContactName'] = name.get("First")
                        if lastname is not None:
                            cleanrecord['ContactName'] += " " + lastname
                    else:
                        cleanrecord['ContactName'] = None

                    ids = parsed['Ids']['Id']
                    for id in ids:
                        cleanrecord['SampleName'] = None
                        cleanrecord['SRA'] = None
                        if (not isinstance(id, OrderedDict)):
                            break
                        else:
                            if id.get("@db_label") == 'Sample name':
                                cleanrecord['SampleName'] = id['#text']
                            if id.get("@db") == 'SRA':
                                cleanrecord['SRA'] = id['#text']

                    recordlist.append(cleanrecord)


    # remove dupe records
    recordlist = {json.dumps(d, sort_keys=True) for d in recordlist}
    recordlist = [json.loads(t) for t in recordlist]
    print("# total record processed :", count)
    print("# total unique records processed :", len(recordlist))
    with open('./dumps/biosample-records.json', 'w') as f: # temp
        json.dump(recordlist, f, indent=4)

    return recordlist

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filepath", help="path to biosample xml folder")
    args = parser.parse_args()
    biosample_XML_to_dicts(args.filepath)