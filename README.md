# 📚 NCBI-Download

Downloads Apoidea-related xmls from NCBI using [entrezpy](https://entrezpy.readthedocs.io) for Beebiome data portal.

## 🧰 Usage

1.  Run pip install -r requirements.txt
2.  Set enviroment variables found in .env.example, or rename to .env with a NCBI API key filled in
3.  Execute main.py

## 🐛 Known quirks
* Download is slow
* XMLs files may contain duplicate records
* NCBI servers may decide to timeout requests randomly
* Buggy behaviour on Windows observed; recommend to run on Linux machine
