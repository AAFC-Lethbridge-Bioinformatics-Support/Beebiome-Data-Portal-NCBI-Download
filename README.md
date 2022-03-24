# NCBI-Download

Downloads Apoidea-related xmls from NCBI using [entrezpy](https://entrezpy.readthedocs.io) for Beebiome data portal.

## Usage

1.  Run pip install -r requirements.txt
2.  Set enviroment variables found in .env.example, or rename to .env with a NCBI API key filled in
3.  Execute main.py

(Note: XMLs files may contain duplicate records)

## TODO
- ~~have it run from one python script~~
- download Nucleotide ids (perl script has this functionality)
- test multi-threading in entrezpy library
- actual logging for xml processing scripts
- log to file
- resolve gap in bioproject xmls between the  perl script
- pushing data to db