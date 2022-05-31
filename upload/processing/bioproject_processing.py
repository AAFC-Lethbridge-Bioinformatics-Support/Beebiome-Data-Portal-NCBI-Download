import logging
import os
import xml.etree.ElementTree as ET
from typing import Dict, List

import toml
import xmltodict
from .processing import Processor


config = toml.load("./config.toml")
logger = logging.getLogger(__name__)

class BioprojectProcessor(Processor):
    def __init__(self, filepath):
        super().__init__(filepath, "bioproject")

    @staticmethod
    def _process_file(filepath):
        # XML parsing looks awful but it works
        clean_records = []
        if filepath.endswith(".xml"):
                with open(os.path.join(filepath), "rb") as openfile:
                    tree = ET.parse(openfile)
                    records = tree.findall(".//DocumentSummary")
                    filecount = 0

                    for record in records:
                        filecount += 1
                        clean_record = {}
                        publication_dict = {}
                        parsed = (xmltodict.parse(ET.tostring(record)))[
                            "DocumentSummary"]
                        project = parsed.get("Project", {})

                        projectid = (project.get(
                            "ProjectID", {}).get("ArchiveID", {}))
                        clean_record["BioprojectAccession"] = projectid.get(
                            "@accession")
                        clean_record["ArchiveID"] = projectid.get("@id")
                        clean_record["Archive"] = projectid.get("@archive")

                        projectdescr = (project.get("ProjectDescr", {}))
                        clean_record["Title"] = projectdescr.get("Title")
                        clean_record["Description"] = projectdescr.get(
                            "Description")


                        clean_record["Relevance"] = None
                        relevance = record.find(
                                "Project/ProjectDescr/Relevance")
                        if relevance is not None:
                            clean_record["Relevance"] = str(
                                ET.tostring((relevance)))

                        organization = (parsed.get("Submission", {}).get(
                            "Description", {}).get("Organization", None))
                        if isinstance(organization, List):
                            # TODO: handle multiple organizations
                            organization = organization[0].get("Name")
                        elif (organization is not None):
                            organization = organization.get("Name")

                        if isinstance(organization, Dict):
                            organization = organization.get("Name")

                        clean_record["Organization"] = organization

                        if (not (isinstance(clean_record["Organization"], str)) and clean_record["Organization"] is not None):
                            logger.error(
                                "Organization was not parsed as a string:\n %s ", clean_record["Organization"])
                            raise RuntimeError(
                                "Organization was not parsed as a string")

                        clean_record["DataTypeSet"] = None
                        datatype = record.find("Project/ProjectType/ProjectTypeSubmission/IntendedDataTypeSet/DataType")
                        if (datatype is not None):
                            clean_record["DataTypeSet"] = str(ET.tostring((datatype)))

                        clean_record["publication"] = None
                        publication = projectdescr.get("Publication")

                        if publication is not None:
                            publication_dict["PublicationDate"] = None
                            publication_dict["DOI"] = None
                            publication_dict["Title"] = None
                            publication_dict["Journal"] = None
                            publication_dict["Authors"] = None

                            if (isinstance(publication, list)):
                                # TODO: handle multiple publications case
                                publication = publication[0]

                            publication_dict["DOI"] = publication.get("@id")
                            clean_record["publication"] = publication_dict["DOI"]
                            if (publication_dict["DOI"]):
                                publication_dict["PublicationDate"] = publication.get(
                                    "@date")

                                citation = publication.get("StructuredCitation")
                                if citation is not None:
                                    publication_dict["Title"] = citation.get(
                                        "Title")
                                    publication_dict["Journal"] = citation.get(
                                        "Journal", {}).get("JournalTitle")

                                    authorset = record.find(
                                        "Project/ProjectDescr/Publication/StructuredCitation/AuthorSet")
                                    if authorset is not None:
                                        publication_dict["Authors"] = str(
                                            ET.tostring((authorset)))

                                clean_record["publication"] = publication_dict

                        clean_records.append(clean_record)
        return clean_records


