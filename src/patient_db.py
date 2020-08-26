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

    def match_patients(self, term, event_keys='', event_types=['']):
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
            total_num_visits += num_visits
        stats = dict()
        stats['avg_num_visits'] = total_num_visits / float(len(self.patients))
        return stats

    def get_event_counters(self, event_types):
        counters = dict()
        items = dict()
        event_roles = set()
        entity_levels = ['patient', 'visit', 'event']
        
        for event_type in event_types:
            if event_type == 'DiagnosisEvent':
                event_type_roles = ['diagnosis_icd9', 'diagnosis_name', 'diagnosis_long_name']
            elif event_type == 'LabEvent':
                event_type_roles = ['test_name', 'test_status', 'test_value']
            elif event_type == 'MedicationEvent':
                event_type_roles = ['dosage', 'duration', 'indication', 'medication']
            elif event_type == 'PatientEvent':
                pass # TODO event_type_roles= ['attribute', 'attribute_value']
            elif event_type == 'ProcedureEvent':
                event_type_roles = ['procedure_icd9', 'procedure_name', 'targeted_organs']
            elif event_type == 'VitalEvent':
                event_type_roles = ['location', 'vital_outcome']

            event_roles.update(event_type_roles)


        # prepare event_counter and event_items
        for entity_level in entity_levels:
            counters[entity_level] = dict()
            items[entity_level] = dict()
            for role in event_roles:
                counters[entity_level][role] = Counter()
                items[entity_level][role] = set()

        patient_items = items['patient']
        visit_items = items['visit']
        event_items = items['event']

        patient_counter = counters['patient']
        visit_counter = counters['visit']
        event_counter = counters['event']
        
        entity_count_per_level = Counter()

        # Iterate through all patients
        for patient in self.patients.values():
            entity_count_per_level['patient'] += 1
            # Clear patient items set
            for role in event_roles:
                patient_items[role].clear() 
            # Iterate through all patient visits
            for visit in patient.visits:
                entity_count_per_level['visit'] += 1
                # Clear visit items set
                for role in event_roles:
                    visit_items[role].clear()
                # Iterate through all visit events
                for event in visit.events:
                    entity_count_per_level['event'] += 1
                    if event.event_type not in event_types:
                        continue
                    # Clear event items
                    for role in event_roles:
                        event_items[role].clear()
                    
                    # Add items
                    for role in event_roles:
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

        return counters, event_roles, entity_levels, entity_count_per_level


def get_top_k(agg_counts, keys, roles, k):
    top_k = dict()
    for key in keys:
        top_k[key] = dict()
        for role in roles:
            top_k[key][role] = agg_counts[key][role].most_common(k)

    return k, top_k
