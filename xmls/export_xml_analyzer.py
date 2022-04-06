import json
import xml.etree.ElementTree
import entrezpy.base.analyzer
import traceback
from .xml_result import XMLResult
import os
from datetime import datetime


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

    # overwrite existing class method for better error dump
    def parse(self, raw_response, request):
        if request.retmode not in super().known_fmts:
            self.logger.error(json.dumps({'unknown format': request.retmode}))
            raise NotImplementedError(f"Unknown format: {request.retmode}")
        try:
            response = self.convert_response(raw_response.read().decode('utf-8'), request)
            if self.isErrorResponse(response, request):
                self.hasErrorResponse = True
                self.analyze_error(response, request)
            else:
                self.analyze_result(response, request)
        except Exception as e:
            with open(f'{self.filepath}/{self.db}-query-{self.query_num}-error.log', "w") as f:
                    f.write(json.dumps({'func':__name__,'request' : request.dump(), 'exception': str(e), 'traceback': traceback.format_exc()}, indent=4))
            self.logger.error(f'Uncaught exception when processing response in query {self.query_num} for {self.db}')
            pass


    # overwrite existing class method for less strict error checking
    def check_error_xml(self, response):
        try:
             xml.etree.ElementTree.fromstring(response.getvalue())
        except  xml.etree.ElementTree.ParseError:
            return True
        return False

    def analyze_error(self, response, request):
        dump = json.dumps({'func':__name__,'request' : request.dump(), 'exception': "Response may not be properly formatted XML", 'traceback': "None",
                                    'response' : response.getvalue()}, indent=4)
        with open(f'{self.filepath}/{self.db}-query-{self.query_num}-error.log', "w") as f:
            f.write(dump)
        self.logger.error(f'Failed converting response to xml in query {self.query_num} for {self.db}')

    def analyze_result(self, response, request):
        self.init_result(response, request)

        output = response.getvalue()
        # timestamp necessary due to querys sometimes being multiple requests
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f'{self.filepath}/{self.db}/{self.db}-{self.query_num}-{timestamp}.xml'
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        self.logger.info(f'Writing {self.db}-query-{self.query_num}-{timestamp}.xml')
        with open(filename, "w", encoding="utf-8") as f:
            f.write(output)
        self.result.push_names(filename)
        self.logger.debug(f'Finished writing {self.db}-query-{timestamp}.xml')