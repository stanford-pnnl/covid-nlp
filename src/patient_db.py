# Patient DB
import json
from data_schema import EntityDecoder

class PatientDB():
    "Database composed of patient->visit->event relationships"
    
    def __init__(self, name="", patients=dict()):
        self.name = name
        self.patients = patients

    def load(self, path):
        with open(path, 'r') as f:
            for l in f:
                patient = json.loads(l, cls=EntityDecoder)
                self.add_patient(patient)
    
    def add_patient(self, patient):
        patient_id = patient.entity_id
        if self.patients.get(patient_id):
            #print(f"Overwriting patient {patient_id}")
            pass
        self.patients[str(patient_id)] = patient

    def match_term(self, term, event_keys='', event_types=['']):
        matched_patients = PatientDB()
        matches = set()
        matched_patient_ids = set()
        matched_visit_ids = set()
        matched_event_ids = set()
        for patient in self.patients.values():
            patient_id = patient.entity_id
            matched_patient = False
            for visit in patient.visits:
                visit_id = visit.entity_id
                for event in visit.events:
                    event_id = event.entity_id
                    if event.event_type not in event_types:
                        continue
                    for key in event_keys:
                        compare_term = event.roles[key]
                        if term == compare_term:
                            matched_patient = True
                            matched_patient_ids.add(patient_id)
                            matched_visit_ids.add(visit_id)
                            matched_event_ids.add(event_id)
                            matches.add((patient_id, visit_id, event_id, key, term))
            if matched_patient:
                matched_patients.add_patient(patient)
        print(f"Term: {term}, matched {len(matched_patients.patients)} patients")
        return matched_patients, matches
                        
    def merge_patients(self, patient_db):
        print(f"Merging patient DBs...")
        # TODO: make graceful
        patient_ids = list(patient_db.patients.keys())
        for patient_id in patient_ids:
            patient = patient_db.patients[patient_id]
            self.add_patient(patient)
