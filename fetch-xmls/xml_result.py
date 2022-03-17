import entrezpy.base.result

# implement the virtual class
class XMLResult(entrezpy.base.result.EutilsResult):
  def __init__(self, response, request):
    super().__init__(request.eutil, request.query_id, request.db)
    self.filenames = set()

  def size(self):
    return len(self.filenames)

  def isEmpty(self):
    if not self.filenames:
      return True
    return False

  def get_link_parameter(self, reqnum=0):
    print("{} has no elink capability".format(self))
    return {}

  def dump(self):
    return {self:{'dump':{'file_names':[x for x in self.filenames],
                              'query_id': self.query_id, 'db':self.db,
                              'eutil':self.function}}}

  def push_names(self, filename):
    self.filenames.update(filename)
