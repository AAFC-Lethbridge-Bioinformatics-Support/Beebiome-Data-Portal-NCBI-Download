import json
import xml.etree.ElementTree
import entrezpy.base.analyzer
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
            template = "An exception of type {0} occurred. Arguments: {1!r}"
            dump = json.dumps({'func':__name__,'request' : request.dump(), 'response' : '', 'exception': template.format(type(e).__name__, e.args)}, indent=4)
            with open(f'query-{self.query_num}-error-{self.run_timestamp}-run.json', "w") as f:
                    f.write(dump)
            self.logger.error(f'Error parsing raw response; see query-{self.query_num}-error-{self.run_timestamp}-run.json')
            quit()

    # overwrite existing class method for less strict error checking
    def check_error_xml(self, response):
        try:
             xml.etree.ElementTree.fromstring(response.getvalue())
        except  xml.etree.ElementTree.ParseError:
            return True
        return False

    def analyze_error(self, response, request):
        dump = json.dumps({'func':__name__,'request' : request.dump(),
                                    'response' : response.getvalue()}, indent=4)
        with open(f'query-{self.query_num}-error-{self.run_timestamp}-run.json', "w") as f:
            f.write(dump)
        self.logger.error(f'Error converting to xml; query-{self.query_num}-error-{self.run_timestamp}-run.json')
        quit()


    def analyze_result(self, response, request):
        self.init_result(response, request)

        output = response.getvalue()
        # datetime.now necessary due to querys sometimes being multiple requests
        filename = f'{self.filepath}/{self.db}/{self.db}-{self.query_num}-{datetime.now()}-.xml'
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        self.logger.info(f'Writing {self.db}-query-{self.query_num}-{datetime.now()}-.xml')
        with open(filename, "w") as f:
            f.write(output)
        self.result.push_names(filename)
        self.logger.debug(f'Finished writing {self.db}-query-{datetime.now()}-.xml')