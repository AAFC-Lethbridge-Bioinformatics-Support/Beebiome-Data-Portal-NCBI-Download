import entrezpy.base.result

# implement the virtual class
class NamesResult(entrezpy.base.result.EutilsResult):
  def __init__(self, response, request):
    super().__init__(request.eutil, request.query_id, request.db)
    self.taxon_names = set()

  def size(self):
    return len(self.taxon_names)

  def isEmpty(self):
    if not self.taxon_names:
      return True
    return False

  def get_link_parameter(self, reqnum=0):
    print("{} has no elink capability".format(self))
    return {}

  def dump(self):
    return {self:{'dump':{'taxon_names':[x for x in self.taxon_names],
                              'query_id': self.query_id, 'db':self.db,
                              'eutil':self.function}}}

  def push_names(self, taxon_names):
    self.taxon_names.update(taxon_names)
