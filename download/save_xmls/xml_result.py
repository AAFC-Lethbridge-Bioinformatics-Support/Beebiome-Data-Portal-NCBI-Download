import entrezpy.base.result

# implement the virtual class (not used but required by entrezpy)
class XMLResult(entrezpy.base.result.EutilsResult):
  def __init__(self, request):
    super().__init__(request.eutil, request.query_id, request.db)

  def size(self):
    return 1  # for entrezpy

  def isEmpty(self):
    return False # for entrezpy

  def get_link_parameter(self, reqnum=0):
    print("{} has no elink capability".format(self))
    return {}

  def dump(self):
    return {self:{'dump':{'query_id': self.query_id, 'db':self.db,
                              'eutil':self.function}}}
