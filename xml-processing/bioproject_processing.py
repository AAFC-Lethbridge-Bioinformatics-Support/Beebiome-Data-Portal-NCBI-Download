import os
import json
from typing import OrderedDict
import xmltodict
import xml.etree.ElementTree as ET
import argparse

def bioproject_XML_to_dicts(filepath):
    recordlist = []
    publicationlist = []
    uniquepublications = set()
    count = 0
    files = os.listdir(filepath)
    for i, filename in enumerate(files):
        if filename.endswith(".xml"):

            with open(os.path.join(filepath, filename), "rb") as openfile:
                print("Processing file " + str(i + 1) + "/" + str(len(files)) + ": " + filename)
                tree = ET.parse(openfile)
                for record in tree.findall(".//DocumentSummary"):
                    count += 1
                    cleanrecord = {}
                    publicationrecord = {}

                    parsed = (xmltodict.parse(ET.tostring(record)))["DocumentSummary"]

                    project = parsed.get("Project", {})


                    projectid = (project.get("ProjectID", {}).get("ArchiveID", {}))
                    cleanrecord["ProjectAccession"] = projectid.get("@accession")
                    cleanrecord["ArchiveID"] = projectid.get("@id")
                    cleanrecord["Archive"] = projectid.get("@archive")


                    projectdescr = (project.get("ProjectDescr", {}))
                    cleanrecord["Title"] = projectdescr.get("Title")
                    cleanrecord["Description"] = projectdescr.get("Description")

                    cleanrecord["Relevance"] = None
                    relevance = record.find('Project').find('ProjectDescr').find('Relevance')
                    if relevance is not None:
                        cleanrecord["Relevance"] = str(ET.tostring((relevance)))

                    submission = parsed.get("Submission", {})
                    organization = (submission.get("Description", {}).get("Organization",{}))
                    cleanrecord["Organization"] = None
                    if (isinstance(organization, OrderedDict)):
                        cleanrecord["Organization"] = organization.get("Name")
                    elif (isinstance(organization, list)):
                        cleanrecord["Organization"] = organization[0].get("Name")

                    datatype = record.find('Project').find('ProjectType').find('ProjectTypeSubmission').find('ProjectDataTypeSet')
                    cleanrecord["DataTypeSet"] = None
                    if datatype is not None:
                        cleanrecord["DataTypeSet"] = str(ET.tostring((datatype)))

                    publication = projectdescr.get("Publication")
                    cleanrecord["DOI"] = None

                    if publication is not None:
                        publicationrecord["PublicationDate"] = None
                        publicationrecord["DOI"] = None

                        if (isinstance(publication, list)):
                            publication = publication[0]

                        publicationrecord["DOI"] = publication.get("@id")
                        cleanrecord["DOI"] = publicationrecord["DOI"]
                        if (publicationrecord["DOI"] not in uniquepublications):
                            publicationrecord["PublicationDate"] = publication.get("@date")

                            citation = publication.get("StructuredCitation")
                            publicationrecord["Title"] = None
                            publicationrecord["Journal"] = None
                            publicationrecord["Authors"] = None

                            if citation is not None:
                                publicationrecord["Title"] = citation.get("Title")
                                publicationrecord["Journal"] = citation.get("Journal", {}).get("JournalTitle")

                                authorset = record.find("Project").find("ProjectDescr").find("Publication").find("StructuredCitation").find("AuthorSet")
                                if authorset is not None:
                                    publicationrecord["Authors"] = str(ET.tostring((authorset)))

                            publicationlist.append(publicationrecord)
                            uniquepublications.add(publicationrecord["DOI"])

                    recordlist.append(cleanrecord)


    print("# total Bioproject record processed :", count)
    with open('./dumps/bioproject-records.json', 'w') as f: # temp
        json.dump(recordlist, f, indent=4)

    with open('./dumps/publication-records.json', 'w') as f: # temp
        json.dump(publicationlist, f, indent=4)

    return recordlist, publicationlist

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filepath", help="path to bioproject xml folder")
    args = parser.parse_args()
    bioproject_XML_to_dicts(args.filepath)