# Patient DB
import json
from data_schema import EntityDecoder, EntityEncoder, Patient
from collections import Counter
from datetime import datetime, timedelta, date
from dateutil import rrule

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

    def dump(self, path):
        """Dump patients KG to a file."""
        print(f"Dumping {len(self.patients)} patients to {path}")

        c: Counter = Counter()
        c['num_keys'] = 0
        c['successful_dumps'] = 0
        c['failed_dumps'] = 0

        with open(path, 'w') as f:
            for key in sorted(self.patients.keys(), key=int):
                c['num_keys'] += 1
                patient = self.patients[key]
                try:
                    patient_str = json.dumps(patient, cls=EntityEncoder)
                    f.write(f"{patient_str}\n")
                    c['successful_dumps'] += 1
                except TypeError as e:
                    c['failed_dumps'] += 1
                    print(f"e: {e}")
                    import pdb
                    pdb.set_trace()
                    #print(f"Failed to dump patient {key}")
        print(f"{c}")

    def generate_patients_from_ids(self, patient_ids):
        """Generate patients from list of IDs."""

        for patient_id in patient_ids:
            patient = Patient(patient_id=str(patient_id))
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
                        
    def attach_visits_to_patients(self, visits, patient_ids):
        num_missing_keys = 0
        num_successful_keys = 0
        patient_ids = [str(x) for x in patient_ids]
        # Attach visits to Patients
        for patient_id in patient_ids:
            patient_visits = visits[patient_id]
            for visit_id, visit in patient_visits.items():
                try:
                    self.patients[str(patient_id)].visits.append(visit)
                    num_successful_keys += 1
                except KeyError:
                    import pdb
                    pdb.set_trace()
                    num_missing_keys += 1
        print(f"Vists, Num missing keys: {num_missing_keys}\n"
              f"Visits, Num successful keys: {num_successful_keys}")

    def merge_patients(self, patients):
        #print(f"Merging patient DBs...")
        # TODO: make graceful
        patient_ids = list(patients.patients.keys())
        for patient_id in patient_ids:
            patient = patients.patients[patient_id]
            self.add_patient(patient)

    def add_demographic_info(self, demographics, use_dask):
        print("Adding demographic info")
        c = Counter()
        c['patients_found'] = 0
        c['patients_not_found'] = 0
        for row in demographics.itertuples():
            person_id = row.person_id
            person_id_key = str(person_id)
            # Does this person exist in the patient DB already?
            if not self.patients.get(person_id_key):
                #import pdb;pdb.set_trace()
                #print(f"Not finding {person_id_key} in patients dict")
                c['patients_not_found'] += 1
                continue
            
            c['patients_found'] += 1
            patient = self.patients[person_id_key]
            patient.date_of_birth = date(row.year_of_birth,
                                     row.month_of_birth,
                                     row.day_of_birth)
            patient.gender = row.gender
            patient.race = row.race
            patient.ethnicity = row.ethnicity
        print(f"{c}")

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
        
        # Point out that items sets are temp

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

    def get_visit_dates(self, time_freq='M'):
        visit_dates = set()
        for patient_id, patient in self.patients.items():
            for visit in patient.visits:
                visit_dates.add(visit.date)
        # Get range start and end values
        min_visit_date = datetime.max
        max_visit_date = datetime.min
        for visit_date in visit_dates:
            if visit_date < min_visit_date:
                min_visit_date = visit_date
            elif visit_date >= max_visit_date:
                max_visit_date = visit_date
       
        start_visit_date = datetime(min_visit_date.year, min_visit_date.month, 1)
        end_visit_date = datetime(max_visit_date.year, max_visit_date.month, 1)
        delta = timedelta()
        for visit_date in rrule.rrule(rrule.MONTHLY, dtstart=start_visit_date, until=end_visit_date):
            print(visit_date)
        import pdb;pdb.set_trace()
        return visit_dates

    def select_date(self, year=None, month=None, day=None):
        match_year=bool(year)
        match_month=bool(month)
        match_day=bool(day)
        date_db = PatientDB()
        for patient_id, patient in self.patients.items():
            patient_match = False
            matched_visits = []
            for visit in patient.visits:
                visit_date = visit.date
                if match_year:
                    if year != visit_date.year:
                        continue
                if match_month:
                    if month != visit_date.month:
                        continue
                if match_day:
                    if day != visit_date.day:
                        continue
                # We have a match!
                #import pdb;pdb.set_trace()
                matched_visits.append(visit)
                patient_match = True
            if patient_match:
                matched_patient = Patient(patient_id=patient_id)
                for matched_visit in matched_visits:
                    matched_patient.visits.append(matched_visit)
                date_db.add_patient(matched_patient)
        return date_db
    
    def get_unique_gender(self):
        unique_genders = set()
        for patient in self.patients.values():
            unique_genders.add(patient.gender)
        return unique_genders

    def get_unique_ethnicities(self):
        unique_ethniciites = set()
        for patient in self.patients.values():
            unique_ethnicities.add(patient.ethnicity)
        return unique_ethnicities

    def get_unique_races(self):
        unique_races = set()
        for patient in self.patients.values():
            unique_races.add(patient.race)
        return unique_races

    def agg_time(self, time_freq='M'):
        # Split patient DB in a DB per each time freq
        visit_dates = self.get_visit_dates(time_freq)
        visit_date_dbs = dict()
        #freq_aggs = self.agg_key(entity_level='Visit', '')
        # iterate through visit dates and create a patientDB per time step
        for visit_date in visit_dates:
            visit_date_db = self.select_date(year=visit_date.year, month=visit_date.month)
            date_key = visit_date.strftime("%Y-%m")
            visit_date_dbs[date_key] = visit_date_db
        
        import pdb;pdb.set_trace()
        return visit_date_dbs

    def agg_ethnicity(self):
        unique_ethnicities = self.get_unique_ethnicities()
        ethnicity_dbs = dict()
        # Create empty ethnicity dbs
        for ethnicity in unique_ethnicities:
            ethnicity_dbs[ethnicity] = PatientDB(name=ethnicity)
        # Put patients in their respective ethnicity dbs
        for patient in self.patients.values():
            ethnicity_dbs[patient.ethnicity].add_patient(patient)
        
        import pdb;pdb.set_trace()
        return ethnicity_dbs

    def agg_gender(self):
        unique_genders = self.get_unique_genders()
        gender_dbs = dict()
        # Create empty gender dbs
        for gender in unique_genders:
            gender_dbs[gender] = PatientDB(name=gender)
        # Put patients in their respective gender dbs
        for patient in self.patients.values():
            gender_dbs[patient.gender].add_patient(patient)
        import pdb;pdb.set_trace()
        return gender_dbs

    def agg_race(self):
        unique_races = self.get_unique_races()
        race_dbs = dict()
        # Create empty race dbs
        for race in unique_races:
            race_dbs[race] = PatientDB(name=race)
        # Put patients in their respective race dbs
        for patient in self.patients.values():
            race_dbs[patient.race].add_patient(patient)
        import pdb;pdb.set_trace()
        return race_dbs

        

def get_top_k(agg_counts, keys, roles, k):
    top_k = dict()
    for key in keys:
        top_k[key] = dict()
        for role in roles:
            top_k[key][role] = agg_counts[key][role].most_common(k)

    return k, top_k
