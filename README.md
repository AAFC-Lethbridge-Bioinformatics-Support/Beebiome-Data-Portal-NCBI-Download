# NCBI-Download

Downloads Apoidea-related xmls from NCBI using [entrezpy](https://entrezpy.readthedocs.io) for Beebiome data portal.

## Usage

1.  Run pip install -r requirements.txt
2.  Set enviroment variables found in .env.example, or rename to .env with a NCBI API key filled in
3.  Be sure to execute Python scripts from the NCBI-Download subdirectory, not the subdirectories itself.
4.  Run fetch_names.py to get the taxon-names.json with all Apoidea names first
5.  Run fetch_xmls.py to get the xmls. (~ 2 hours in my case)

## TO-DO
- have it run from one python script
- download Nucleotide ids (perl script has this functionality)
- test multi-threading in entrezpy library
- actual logging for xml processing scripts