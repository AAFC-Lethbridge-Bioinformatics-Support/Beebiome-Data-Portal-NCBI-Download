# 📚 NCBI-Download

### This is an alternative download method that was developed and is not used currently
Downloads biosamples and biosample-related xmls from NCBI using forked [entrezpy](https://entrezpy.readthedocs.io "Entrezpy offical docs") for Beebiome data portal.

## 🧰 Usage

1.  Use **git clone --recursive** on this repository to get the whole project.
2.  Run pip install -r requirements.txt to get the dependencies. (recommended to do it in a Python virtual environment)
3.  Make copy of config.toml.example and rename it to config.toml with an **[NCBI API key](https://www.ncbi.nlm.nih.gov/account/settings/ "Generate a key here") filled in.**
4.  Execute main.py using Python 3.6 or later from the NCBI-Download directory.

## 🐛 Known quirks
* Download is slow - try enabling threading (see config.toml)
* XMLs files may contain duplicate records

