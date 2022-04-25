import json
from .names_result import NamesResult
import entrezpy.base.analyzer
import xml.etree.ElementTree

# implement the virtual class
class NamesAnalyzer(entrezpy.base.analyzer.EutilsAnalyzer):
    def __init__(self):
        super().__init__()

    def init_result(self, response, request):
        if self.result is None:
            self.result = NamesResult(response, request)

    def analyze_error(self, response, request):
        print(json.dumps({__name__:{'Response': {'dump' : request.dump(),
                                    'error' : response.getvalue()}}}))

    def analyze_result(self, response, request):
        self.init_result(response, request)
        tree = xml.etree.ElementTree.fromstring(response.getvalue())

        for taxon in tree.findall('Taxon'):
            new_names = set()
            other_names = taxon.find('OtherNames')
            new_names.add(taxon.find("ScientificName").text)
            if (other_names is not None):
                if (other_names.find('CommonName') is not None):
                    for elm in other_names.findall('CommonName'):
                        new_names.add(elm.text)
                if (other_names.find('GenbankCommonName') is not None):
                    for elm in other_names.findall('GenbankCommonName'):
                        new_names.add(elm.text)
                if (other_names.find('Synonym') is not None):
                    for elm in other_names.findall('Synonym'):
                        new_names.add(elm.text)
            if len(new_names) > 0:
                self.result.push_names(new_names)


