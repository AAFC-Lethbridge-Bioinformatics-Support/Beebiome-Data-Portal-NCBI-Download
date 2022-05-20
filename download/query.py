import json
import logging
from multiprocessing import Process

from download.save_xmls.xml_analyzer import SaveXMLs

logger = logging.getLogger(__name__)


class Query(Process):
    """ Class for a single Query
        Will get Biosample/Bioproject UIDs and linked SRA XMLs from the specified query
    """

    def __init__(self, ncbi_connection, filepath, query, index):
        super().__init__()
        self.ncbi_connection = ncbi_connection
        self.filepath = filepath
        self.query = query
        self.index = index

    def run(self):
        biosample_uids = self.get_biosample_uids()

        if len(biosample_uids) == 0:
            return logger.warning(f'No biosample UIDs found for query {self.index}')
        biosample_uids = list(set(biosample_uids))

        self.get_sra(biosample_uids)

        with open(f"{self.filepath}/biosamples/biosample_UIDs.json", "r") as file:
            all_biosample_uids = json.load(file)

        all_biosample_uids.extend(biosample_uids)

        with open(f"{self.filepath}/biosamples/biosample_UIDs.json", "w") as file:
            json.dump(sorted(list(set(all_biosample_uids))), file)

        bioproject_uids = self.get_bioproject_uids()

        with open(f"{self.filepath}/bioprojects/bioproject_UIDs.json", "r") as file:
            bioproject_all_uids = json.load(file)

        bioproject_all_uids.extend(bioproject_uids)

        with open(f"{self.filepath}/bioprojects/bioproject_UIDs.json", "w") as file:
            json.dump(sorted(list(set(bioproject_all_uids))), file)

        return

    def get_biosample_uids(self):
        logger.info("Getting biosample UIDs")
        pipeline = self.ncbi_connection.new_pipeline()
        pipeline.add_search({'db': 'biosample',
                            'term': self.query,
                             'rettype': 'uilist'})
        analzyer = self.ncbi_connection.run(pipeline)
        return sorted(analzyer.result.uids)

    def get_bioproject_uids(self):
        logger.info("Getting bioproject UIDs")
        pipeline = self.ncbi_connection.new_pipeline()
        biosamples = pipeline.add_search({'db': 'biosample',
                                          'term': self.query,
                                          'rettype': 'count'})
        linked = pipeline.add_link(
            {'db': 'bioproject', 'cmd': 'neighbor_history'}, dependency=biosamples)
        pipeline.add_search({'rettype': 'uilist'}, dependency=linked)
        analzyer = self.ncbi_connection.run(pipeline)
        return sorted(analzyer.result.uids)

    def get_sra(self, uids):
        """
            Find and downloads SRA for given list of biosample UIDs

            Split UIDs into chunks due to limitations of entrezpy or NCBI?
        """

        logging.info("Getting SRA XMLs")
        size = 1000
        if len(uids) >= size:
            chunked = list(self.divide_chunks(uids, size))
        else:
            chunked = [uids]

        total_chunks = len(chunked)
        procs = []

        for index, chunk in enumerate(chunked):
            index += 1

            if (total_chunks > 1):
                filenamenum = (str(self.index) + "-" + str(index))
            else:
                filenamenum = str(self.index)

            chunk = list(set(chunk))
            proc = Process(target=self.get_sra_helper, args=(
                self.ncbi_connection, self.filepath, filenamenum, chunk))
            procs.append(proc)

        for index, proc in enumerate(procs):
            index += 1
            logger.info(
                f'Running SRA subquery {index} of {total_chunks} for query {self.index}')
            proc.start()
            proc.join()
            if proc.exitcode != 0:
                logging.error(
                    "SRA subquery {index} for query {query_num} failed")
                raise RuntimeError("Children process exited unexpectedly")
            proc.close()
        return

    def divide_chunks(self, l, n):
        """ return list in chunks of n """
        for i in range(0, len(l), n):
            yield l[i:i + n]

    @staticmethod
    def get_sra_helper(ncbi_connection, filepath, filenamenum, ids):
        """ Ran as a process to manage mem/thread usage"""
        pipeline = ncbi_connection.new_pipeline()
        link_results = pipeline.add_link(
            {'dbfrom': 'biosample', 'db': 'sra', 'cmd': 'neighbor', 'id': ids, 'link': False})
        pipeline.add_fetch({'retmode': 'xml'}, dependency=link_results,  analyzer=SaveXMLs(
            dbname='sra', query_num=filenamenum, filepath=filepath))
        ncbi_connection.run(pipeline)
        return
