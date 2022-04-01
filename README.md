# 📚 NCBI-Download

Downloads Apoidea-related xmls from NCBI using [entrezpy](https://entrezpy.readthedocs.io) for Beebiome data portal.

## 🧰 Usage

1.  Use git clone --recursive on this repository to get the whole project.
2.  Run pip install -r requirements.txt to get the dependencies.
3.  Make a copy of .env.example called .env with a NCBI API key (and any other relevant variables) filled in.
4.  Execute main.py using Python 3.6 or later.

## 🐛 Known quirks
* Download is slow
* XMLs files may contain duplicate records
* NCBI servers may decide to timeout requests randomly
* Buggy behaviour on Windows observed; recommend to run on Linux machine
