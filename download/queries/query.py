import logging
from multiprocessing import Process

from .save_xmls.xml_analyzer import SaveXMLs

logger = logging.getLogger(__name__)


def divide_chunks(l, n):
    """ return list in chunks of n """
    for i in range(0, len(l), n):
        yield l[i:i + n]

class Query(Process):
    def __init__(self, ncbi_connection, filepath, query, index):
        super().__init__()
        self.ncbi_connection = ncbi_connection
        self.filepath = filepath
        self.query = query
        self.index = index

    def run(self):
        config = (self.ncbi_connection, self.filepath, self.query, self.index)

        proc = Process(target=self._get_biosamples_xmls, args=(config,))
        proc.start()
        proc.join()
        if proc.exitcode != 0:
            logger.error("Error in query " +
                         str(self.index) + " for biosample")
            raise RuntimeError("Children process exited unexpectedly")

        proc = Process(target=self._get_related_xmls, args=(config,))
        proc.start()
        proc.join()
        if proc.exitcode != 0:
            logger.error("Error in query " +
                         str(self.index) + " for biosample")
            raise RuntimeError("Children process exited unexpectedly")
        return

    @staticmethod
    def _get_biosamples_xmls(config):
        (ncbi_connection, filepath, query, index) = config
        pipeline = ncbi_connection.new_pipeline()

        biosample_result = pipeline.add_search(
            {'db': 'biosample', 'term': query, 'rettype': 'uilist'})
        pipeline.add_fetch({'retmode': 'xml'}, dependency=biosample_result,  analyzer=SaveXMLs(
            dbname="biosample", query_num=index, filepath=filepath))
        ncbi_connection.run(pipeline)
        return

    @staticmethod
    def _get_related_xmls(config):
        (_, _, _, bigindex) = config
        uids = sorted(list(set(Query.get_biosample_uids(config))))

        size = 1000
        if len(uids) >= size:
            chunks = list(divide_chunks(uids, size))
        else:
            chunks = [uids]

        total_chunks = len(chunks)
        for index, chunk in enumerate(chunks):
            index += 1
            logger.info(
                f'Running related subqueries {index} of {total_chunks} for query {bigindex}')
            chunk = list(set(chunk))

            proc = Process(target=Query.get_db_xmls, args=(config, 'sra', chunk))
            proc.start()
            proc.join()
            if proc.exitcode != 0:
                logger.error("Error in query " +
                            str(bigindex) + " for SRA")
                raise RuntimeError("Children process exited unexpectedly")

            proc2 = Process(target=Query.get_db_xmls, args=(config, 'bioproject', chunk))
            proc2.start()
            proc2.join()
            if proc2.exitcode != 0:
                logger.error("Error in query " +
                            str(bigindex) + " for bioproject")
                raise RuntimeError("Children process exited unexpectedly")

    @staticmethod
    def get_biosample_uids(config):
        (ncbi_connection, _, query, _) = config
        pipeline = ncbi_connection.new_pipeline()
        pipeline.add_search({'db': 'biosample',
                            'term': query,
                                'rettype': 'uilist'})
        logger.info("Getting biosample UIDs")
        analzyer = ncbi_connection.run(pipeline)
        return sorted(analzyer.result.uids)

    @staticmethod
    def get_db_xmls(config, db, chunk):
        (ncbi_connection, filepath, _, index) = config
        pipeline = ncbi_connection.new_pipeline()
        link_results = pipeline.add_link(
            {'dbfrom': 'biosample', 'db': db, 'cmd': 'neighbor', 'id': chunk, 'link': False})
        pipeline.add_fetch({'retmode': 'xml'}, dependency=link_results,  analyzer=SaveXMLs(
            dbname=db, query_num=index, filepath=filepath))
        ncbi_connection.run(pipeline)
        return
