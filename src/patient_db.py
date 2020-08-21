# Patient DB
import json
from data_schema import EntityDecoder

class PatientDB():
    "Database composed of patient->visit->event relationships"
    
    def __init__(self, name="", patients=None)
    self.name = name
    self.patients = patients

    def load(self, path):
        with open(path, 'r') as f:
            for l in f:
                patient = json.loads(l, cls=EntityDecoder)
                import pdb;pbd.set_trace()

