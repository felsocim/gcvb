import gcvb.yaml_input as yaml_input
import gcvb.db as db

class BaseLoader(object):
    def __init__(self):
        self.loaded = {}
    def load_base(self, run_id):
        ya,mod = db.retrieve_input(run_id)
        if (ya,mod) not in self.loaded:
            self.loaded[(ya,mod)] = yaml_input.load_yaml(ya,mod)
        return self.loaded[(ya,mod)]

loader = BaseLoader()
