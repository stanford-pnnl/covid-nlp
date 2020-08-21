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
                self.patients[str(patient_id)] = patient
    
    def add_patient(self, patient):
        patient_id = patient.entity_id
        if self.patients.get(patient_id):
            #print(f"Overwriting patient {patient_id}")
            pass
        self.patients[str(patient_id)] = patient

    def match_term(self, term, event_types=['']):
        matched_patients = PatientDB()
        for patient in self.patients.values():
            matched_patient = False
            for visit in patient.visits:
                if matched_patient:
                    break
                for event in visit.events:
                    if matched_patient:
                        break
                    if event.event_type not in event_types:
                        continue 
                    compare_term = event.roles['diagnosis_name']
                    if term == compare_term:
                        matched_patients.add_patient(patient)
                        matched_patient = True
        print(f"Term: {term}, matched {len(self.patients)} patients")
        return matched_patients
                        
    def merge_patients(self, patient_db):
        print(f"Merging patient DBs...")
        # TODO: make graceful
        patient_ids = list(patient_db.patients.keys())
        for patient_id in patient_ids:
            patient = patient_db.patients[patient_id]
            self.add_patient(patient)
