# NCBI-Download

Download Apoidea-related xmls from NCBI using [entrezpy](https://entrezpy.readthedocs.io).

## Usage

1.  Install requirements.txt using pip
2.  Set enviroment variables in .env.example or rename to .env, with a NCBI API key filled in
3.  Execute Python scripts from the Download directory, not the subdirectory.
4.  Run fetch-names.py to get the taxon_name.json with all Apoidea names first
5.  Run fetch-xml.py to get the xmls. (~ 2 hours in my case)

## TODO
- have it run from one python script
- download Nucleotide ids (perl script has this functionality)
- test multi-threading in entrezpy library