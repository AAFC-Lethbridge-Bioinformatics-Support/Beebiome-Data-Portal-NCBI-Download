import datetime
import json
import xml.etree.ElementTree
import entrezpy.base.analyzer
from .xml_result import XMLResult
import os

# implement the virtual class
class ExportXML(entrezpy.base.analyzer.EutilsAnalyzer):

    def __init__(self, dbname="", query_num=0, filepath="."):
        super().__init__()
        self.db = dbname
        self.query_num = query_num
        self.filepath = filepath

    def init_result(self, response, request):
        if self.result is None:
            self.result = XMLResult(response, request)

    # overwrite existing class method for less strict error checking
    def check_error_xml(self, response):
        try:
             xml.etree.ElementTree.fromstring(response.getvalue())
        except  xml.etree.ElementTree.ParseError:
            return True
        return False

    def analyze_error(self, response, request):
        dump = json.dumps({'func':__name__,'request' : request.dump(), 'exception': "Error in response", 'traceback': "None",
                                    'response' : response.getvalue()}, indent=4)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        with open(f'{self.filepath}/{self.db}-query-{self.query_num}-error_({timestamp}).log', "w") as f:
            f.write(dump)
        self.logger.error(f'Failed converting response to xml in query {self.query_num} for {self.db}')

    def analyze_result(self, response, request):
        self.init_result(response, request)
        output = response.getvalue()

        filename = f'{self.filepath}/{self.db}/{self.db}-{self.query_num}.xml'
        subquery = 0
        while os.path.exists(filename):
            subquery += 1
            filename = f'{self.filepath}/{self.db}/{self.db}-{self.query_num}-{subquery}.xml'

        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w", encoding="utf-8") as f:
            self.logger.debug(f'Writing {filename}')
            f.write(output)
        self.result.push_names(filename)
