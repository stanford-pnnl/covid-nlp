# Patient DB
import json
from data_schema import EntityDecoder
from collections import Counter

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
        return matched_patients, matches
                        
    def merge_patients(self, patient_db):
        #print(f"Merging patient DBs...")
        # TODO: make graceful
        patient_ids = list(patient_db.patients.keys())
        for patient_id in patient_ids:
            patient = patient_db.patients[patient_id]
            self.add_patient(patient)

    def get_stats(self):
        total_num_visits = 0
        for patient_id, patient in self.patients.items():
            num_visits = len(patient.visits)
            #print(f"num_visits: {num_visits}")
            if num_visits > 1:
                print(f"Found a patient with {num_visits} visits")
            total_num_visits += num_visits
        stats = dict()
        stats['avg_num_visits'] = total_num_visits / float(len(self.patients))
        return stats

    def get_diagnosis_event_info(self):
        diagnosis_counter = dict()
        diagnosis_items = dict()
        entity_levels = ['patient', 'visit', 'event']
        diagnosis_event_roles = ['diagnosis_icd9', 'diagnosis_name']

        # prepare diagnosis_counter and diagnosis_items
        for entity_level in entity_levels:
            diagnosis_counter[entity_level] = dict()
            diagnosis_items[entity_level] = dict()
            for role in diagnosis_event_roles:
                diagnosis_counter[entity_level][role] = Counter()
                diagnosis_items[entity_level][role] = set()

        patient_items = diagnosis_items['patient']
        visit_items = diagnosis_items['visit']
        event_items = diagnosis_items['event']

        patient_counter = diagnosis_counter['patient']
        visit_counter = diagnosis_counter['visit']
        event_counter = diagnosis_counter['event']

        # Iterate through all patients
        for patient in self.patients.values():
            # Clear patient items set
            for role in diagnosis_event_roles:
                patient_items[role].clear() 
            # Iterate through all patient visits
            for visit in patient.visits:
                # Clear visit items set
                for role in diagnosis_event_roles:
                    visit_items[role].clear()
                # Iterate through all visit events
                for event in visit.events:
                    # Only check DiagnosisEvents
                    if event.event_type != 'DiagnosisEvent':
                        continue
                    # Clear event items
                    for role in diagnosis_event_roles:
                        event_items[role].clear()
                    
                    # Add items
                    for role in diagnosis_event_roles:
                        item = event.roles[role]
                        # Add item to all class sets
                        event_items[role].add(item)
                        visit_items[role].add(item)
                        patient_items[role].add(item)
                         
                    # Count event items
                    for role, items in event_items.items():
                        for item in items:
                            event_counter[role][item] += 1
                # Count visit items
                for role_key, items in visit_items.items():
                    for item in items:
                        visit_counter[role_key][item] += 1
            # Count patient items
            for role_key, items in patient_items.items():
                for item in items:
                    patient_counter[role_key][item] += 1

        return diagnosis_counter

def get_top_k(agg_counts, keys, roles, k):
    top_k = dict()
    for key in keys:
        top_k[key] = dict()
        for role in roles:
            top_k[key][role] = agg_counts[key][role].most_common(k)

    return top_k
