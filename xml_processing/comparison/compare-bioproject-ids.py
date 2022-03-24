import os
import json
import xmltodict
import xml.etree.ElementTree as ET

filepath = "downloaded-XMLs/run-2022-03-23_14:56:44/bioproject"
recordlist = []
count = 0
files = os.listdir(filepath)
for i, filename in enumerate(files):
    if filename.endswith(".xml"):
        with open(os.path.join(filepath, filename), "rb") as openfile:
            print("Processing file " + str(i + 1) + "/" + str(len(files)) + ": " + filename)
            try:
                tree = ET.parse(openfile)
                for record in tree.findall(".//DocumentSummary"):
                    count += 1
                    parsed = (xmltodict.parse(ET.tostring(record)))['DocumentSummary']
                    project = parsed.get("Project", {})
                    projectdescr = (project.get("ProjectDescr", {}))

                    projectid = (project.get("ProjectID", {}).get("ArchiveID", {}))
                    recordlist.append(str(projectid.get("@accession")) + " " + str(projectdescr.get("Title")))


            except ET.ParseError as e:
                print("Error parsing file " + filename)
                continue


print("# total record processed #1:", count)


filepath = "./downloaded-XMLs/Apoidea/bioproject/"
recordlist2 = []
count2 = 0
files = os.listdir(filepath)
for i, filename in enumerate(files):
    if filename.endswith(".xml"):
        with open(os.path.join(filepath, filename), "rb") as openfile:
            print("Processing file " + str(i + 1) + "/" + str(len(files)) + ": " + filename)
            try:
                tree = ET.parse(openfile)
                for record in tree.findall(".//DocumentSummary"):
                    count2 += 1

                    parsed = (xmltodict.parse(ET.tostring(record)))['DocumentSummary']
                    project = parsed.get("Project", {})
                    projectdescr = (project.get("ProjectDescr", {}))


                    projectid = (project.get("ProjectID", {}).get("ArchiveID", {}))
                    recordlist2.append(str(projectid.get("@accession")) + " " + str(projectdescr.get("Title")))


            except ET.ParseError as e:
                print("Error parsing file " + filename)
                continue

print("# total record processed #2:", count2)

recordlist = set(recordlist)
recordlist2 = set(recordlist2)

print("Number of records in #1:", len(recordlist))
print("Number of records in #2:", len(recordlist2))

difference = set(recordlist) - set(recordlist2)

print("# of records in #1 but not in #2:", len(difference))

with open('./dumps/added_bioproject_difference.json', 'w') as f:
    json.dump(list(difference), f, indent=4)

difference2= set(recordlist2) - set(recordlist)
print("# of records in #2 but not in #1:", len(difference2))

with open('./dumps/missing_bioproject_difference.json', 'w') as f:
    json.dump(list(difference2), f, indent=4)