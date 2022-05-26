import logging
import xml.etree.ElementTree as ET
from typing import Dict, List

import toml
import xmltodict
from .XMLDocument import XMLDocument


config = toml.load("./config.toml")
logger = logging.getLogger(__name__)


class BioprojectDocument(XMLDocument):
    def __init__(self, filepath):
        super().__init__(filepath, "bioproject")

    def _parse(self):
        """ Parse the XML file and return a list of records

            Returns:
                A list of records
        """
        record_list = []
        needed_ids = self.ids.copy()

        for _, elm in ET.iterparse(open(self.filepath, "rb")):
            if elm.tag == "Package":
                # elem is record so parse entire element, XML parse looks messy but works
                record = elm.find("Project")
                clean_record = {}
                parsed = (xmltodict.parse(ET.tostring(record)))["Project"]
                project = parsed.get("Project", {})
                projectid = (project.get(
                    "ProjectID", {}).get("ArchiveID", {}))

                clean_record["ArchiveID"] = projectid.get("@id")

                if clean_record["ArchiveID"] not in self.ids:
                    logging.debug("Skipping Bioproject record",
                                  clean_record["ArchiveID"])
                    elm.clear()
                    continue
                else:
                    needed_ids.remove(clean_record["ArchiveID"])

                clean_record["BioprojectAccession"] = projectid.get(
                    "@accession")

                clean_record["Archive"] = projectid.get("@archive")

                projectdescr = (project.get("ProjectDescr", {}))
                clean_record["Title"] = projectdescr.get("Title")
                clean_record["Description"] = projectdescr.get(
                    "Description")

                relevance = None
                if record.find("Project/ProjectDescr/Relevance") is not None:
                    relevance = record.find(
                        "Project/ProjectDescr/Relevance")
                if relevance is not None:
                    clean_record["Relevance"] = str(
                        ET.tostring((relevance)))
                else:
                    clean_record["Relevance"] = None

                organization = (parsed.get("Submission", {}).get(
                    "Description", {}).get("Organization"))
                if isinstance(organization, List):
                    # TODO: handle multiple organizations
                    organization = organization[0].get("Name")
                else:
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
                datatype = None
                if (record.find("Project/ProjectType/ProjectTypeSubmission/IntendedDataTypeSet/DataType") is not None):
                    datatype = record.find(
                        "Project/ProjectType/ProjectTypeSubmission/IntendedDataTypeSet/DataType")
                    if datatype is not None:
                        clean_record["DataTypeSet"] = str(
                            ET.tostring((datatype)))

                clean_record["publication"] = None
                publication = projectdescr.get("Publication")

                if publication is not None:
                    clean_record["publication"] = self._parsePublication(
                        clean_record, publication)

                record_list.append(clean_record)
                elm.clear()
            if (len(needed_ids) == 0):
                break

        if (len(needed_ids) > 0):
            logging.warning(
                "Could not find Bioproject records for %s", needed_ids)
        return record_list

    def _parsePublication(record, publicationdata):
        publication = {}
        publication["PublicationDate"] = None
        publication["DOI"] = None
        publication["Title"] = None
        publication["Journal"] = None
        publication["Authors"] = None

        if (isinstance(publicationdata, list)):
            # TODO: handle multiple publications case
            publicationdata = publicationdata[0]

        publication["DOI"] = publicationdata.get("@id")
        record["publication"] = publication["DOI"]

        if (publication["DOI"]):
            publication["PublicationDate"] = publicationdata.get(
                "@date")

            citation = publication.get("StructuredCitation")
            if citation is not None:
                publication["Title"] = citation.get(
                    "Title")
                publication["Journal"] = citation.get(
                    "Journal", {}).get("JournalTitle")

                authorset = record.find(
                    "Project/ProjectDescr/Publication/StructuredCitation/AuthorSet")
                if authorset is not None:
                    publication["Authors"] = str(
                        ET.tostring((authorset)))
        return publication
