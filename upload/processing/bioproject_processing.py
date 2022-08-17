import json
import logging
import os
import xml.etree.ElementTree as ET
from typing import Dict, List, OrderedDict

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
        def process_pub(publication):
            publication_dict = {}
            publication_dict["Title"] = None
            publication_dict["Journal"] = None
            publication_dict["Year"] = None
            publication_dict["Volume"] = None
            publication_dict["Issue"] = None
            publication_dict["PageStart"] = None
            publication_dict["PageEnd"] = None
            publication_dict["ExternalID"] = publication.get("@id")
            publication_dict["Catalogue"] = publication.get("DbType")
            publication_dict["Status"] = publication.get("@status")
            publication_dict["authors"] = []

            citation = publication.get("StructuredCitation")
            if citation is None:
                return publication_dict

            publication_dict["Title"] = citation.get(
                "Title")

            journal = citation.get("Journal", {})
            publication_dict["Journal"] = journal.get("JournalTitle")
            publication_dict["Year"] = journal.get("Year")

            #TODO: handle non-int values
            volume = journal.get("Volume")
            try:
                publication_dict["Volume"] = int(volume)
            except (ValueError, TypeError):
                pass
            issue = journal.get("Issue")
            try:
                publication_dict["Issue"] = int(issue)
            except (ValueError, TypeError):
                pass
            pagesstart = journal.get("PagesFrom")
            try:
                publication_dict["PageStart"] = int(pagesstart)
            except (ValueError, TypeError):
                pass
            pagesend = journal.get("PagesTo")
            try:
                publication_dict["PageEnd"] = int(pagesend)
            except (ValueError, TypeError):
                pass


            authorset = citation.get(
                "AuthorSet", {}).get("Author")

            if authorset is not None:
                authors = []
                if (isinstance(authorset, list)):
                    for author in authorset:
                        parsed_author = {}
                        parsed_author["FirstName"] = author.get(
                            "Name", {}).get("First")
                        parsed_author["LastName"] = author.get(
                            "Name", {}).get("Last")
                        parsed_author["Organization"] = author.get(
                            "Consortium", None)
                        authors.append(parsed_author)
                else:
                    parsed_author = {}
                    parsed_author["FirstName"] = authorset.get(
                        "Name", {}).get("First")
                    parsed_author["LastName"] = authorset.get(
                        "Name", {}).get("Last")
                    parsed_author["Organization"] = authorset.get(
                        "Consortium", None)
                    authors.append(parsed_author)
                publication_dict["authors"] = authors
            return publication_dict

        # XML parsing looks awful but it works
        records = []
        if filepath.endswith(".xml"):
            with open(os.path.join(filepath), "rb") as openfile:
                tree = ET.parse(openfile)
                elements = tree.findall(".//DocumentSummary")
                filecount = 0

                for elem in elements:
                    filecount += 1
                    record = {}
                    parsed = (xmltodict.parse(ET.tostring(elem)))[
                        "DocumentSummary"]
                    project = parsed.get("Project", {})

                    if project:
                        projectid = (project.get(
                            "ProjectID", {}).get("ArchiveID", {}))
                        record["BioprojectAccession"] = projectid.get(
                            "@accession")
                        record["ArchiveID"] = projectid.get("@id")
                        record["ArchiveType"] = projectid.get("@archive")

                        projectdescr = (project.get("ProjectDescr", {}))
                        record["Name"] = projectdescr.get("Title")
                        record["Description"] = projectdescr.get(
                            "Description")

                        externallink = projectdescr.get("ExternalLink", {})
                        record["externallinks"] = []
                        if externallink:
                            if isinstance(externallink, list):
                                for link in externallink:
                                    parsed_link = {}
                                    parsed_link["Name"] = link.get("@label")
                                    parsed_link["URL"] = link.get("URL")
                                    parsed_link["Category"] = link.get("@category")
                                    record["externallinks"].append(parsed_link)
                            elif isinstance(externallink, dict) or isinstance(externallink, OrderedDict):
                                parsed_link = {}
                                parsed_link["Name"] = link.get("@label")
                                parsed_link["URL"] = link.get("URL")
                                parsed_link["Category"] = link.get("@category")
                                record["externallinks"].append(parsed_link)

                        locusprefix = projectdescr.get("LocusTagPrefix")
                        record["locusprefixes"] = []
                        if locusprefix:
                            parsed_prefixes = []
                            if isinstance(locusprefix, list):
                                for prefix in locusprefix:
                                    parsed_prefix = {}
                                    if (isinstance(prefix, Dict)):
                                        parsed_prefix["LocusTagPrefix"] = prefix.get(
                                            "#text")
                                        parsed_prefix["BiosampleID"] = prefix.get(
                                            "@biosample_id")
                                    if (parsed_prefix["LocusTagPrefix"]):
                                        parsed_prefixes.append(parsed_prefix)
                            elif isinstance(locusprefix, dict) or isinstance(locusprefix, OrderedDict):
                                parsed_prefix = {}
                                parsed_prefix["LocusTagPrefix"] = locusprefix.get(
                                    "#text")
                                parsed_prefix["BiosampleID"] = locusprefix.get(
                                    "@biosample_id")
                                if (parsed_prefix["LocusTagPrefix"]):
                                    parsed_prefixes.append(parsed_prefix)
                            else:
                                pass
                            record["locusprefixes"] = parsed_prefixes

                        record["publication"] = []
                        publication = projectdescr.get("Publication")

                        if publication is not None:
                            if isinstance(publication, list):
                                for pub in publication:
                                    record["publication"].append(
                                        process_pub(pub))
                            else:
                                record["publication"].append(process_pub(
                                    publication))

                        submission = parsed.get("Submission")
                        record["Submitter"] = None
                        record["SubmissionID"] = None
                        record["DateSubmitted"] = None
                        record["DateUpdated"] = None
                        if (submission):
                            organization = (parsed.get("Submission", {}).get(
                                "Description", {}).get("Organization", None))
                            if isinstance(organization, List):
                                # TODO: handle multiple organizations
                                organization = organization[0].get("Name")
                            elif (organization is not None):
                                organization = organization.get("Name")

                            if isinstance(organization, Dict):
                                organization = organization.get("Name")

                            record["Submitter"] = organization

                            if (not (isinstance(record["Submitter"], str)) and record["Submitter"] is not None):
                                logger.error(
                                    "Organization was not parsed as a string:\n %s ", record["Submitter"])
                                raise RuntimeError(
                                    "Organization was not parsed as a string")

                            record["SubmissionID"] = submission.get(
                                "@submission_id")
                            record["DateSubmitted"] = submission.get(
                                "@submitted")
                            record["DateUpdated"] = submission.get(
                                "@last_update")

                        records.append(record)
        savelocation = os.path.join(os.path.dirname(filepath), "jsons")
        if not os.path.exists(savelocation):
            os.makedirs(savelocation, exist_ok=True)
        savefile = os.path.join(savelocation, ("%s.json" % (
            os.path.basename(filepath).split(".")[0])))
        with open(savefile, 'w') as f:
            json.dump(records, f, indent=4)
            f.flush()
        return
