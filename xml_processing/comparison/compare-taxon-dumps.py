import json

with open('taxon-names.json', 'r') as f:
  new = json.load(f)

with open('perl-taxon-names.json', 'r') as f:
  old = json.load(f)

print("are the list same length? " + str(len(new) == len(old)))

print("diff for :" + " " + str(list(set(new) - set(old))))
print("missing from new script: " + " " + str(list(set(old) - set(new))))