# Patient DB
import json
import random
import sys
import time
from collections import Counter, namedtuple
from datetime import date, datetime, timedelta
from typing import Any, Optional, Set

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from dateutil import rrule

from data_schema import EntityDecoder, EntityEncoder, Event, Patient, Visit

Match = namedtuple(
    'Match', ['patient_id', 'visit_id', 'event_id', 'event_type', 'role', 'term'])


def date_str_to_obj(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    return date_obj


def now_str():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

#### RANKING FUNCTIONS ###


def get_top_k(agg_counts, entity_levels, event_type_roles, k):
    top_k = dict()
    for entity_level in entity_levels:
        top_k[entity_level] = dict()
        for event_type, event_roles in event_type_roles.items():
            top_k[entity_level][event_type] = dict()
            for event_role in event_roles:
                top_k[entity_level][event_type][event_role] = \
                    agg_counts[entity_level][event_type][event_role].\
                    most_common(k)

    return k, top_k

#### MATCHES FUNCTIONS ###


def get_unique_match_ids(matches):
    matched_ids = dict()
    matched_ids['patient'] = Counter()
    matched_ids['visit'] = Counter()
    matched_ids['event'] = Counter()
    for match in matches:
        patient_id, visit_id, event_id, event_type, event_role, term = match
        matched_ids['patient'][patient_id] += 1
        matched_ids['visit'][visit_id] += 1
        matched_ids['event'][event_id] += 1
    return matched_ids


def get_unique_match_patient_visits(matches):
    patient_visits = dict()
    for match in matches:
        patient_id = match.patient_id
        visit_id = match.visit_id
        # Create counter for patient if we haven't seen
        # that patient yet
        if not patient_visits.get(patient_id):
            patient_visits[patient_id] = Counter()
        patient_visits[patient_id][visit_id] += 1
    return patient_visits


class PatientDB():
    "Database composed of patient->visit->event relationships"

    def __init__(self, name=""):
        self.name = name

        # Internal data
        self.data = dict()
        self.data['patients'] = dict()
        self.data['visits'] = dict()
        self.data['events'] = dict()

        # Aliases
        self.patients = self.data['patients'].values()
        self.visits = self.data['visits'].values()
        self.events = self.data['events'].values()

    def __str__(self):
        s = f"PatientDB(name: {self.name}, "
        s += f"num_patients: {self.num_patients()}, "
        s += f"num_visits: {self.num_visits()}, "
        s += f"num_events: {self.num_events()}, "
        s += f"gender_counts: {self.gender_counts()}, "
        s += f"event_type_counts: {self.get_count_event_types()})"
        return s

    def reproduce(self, name=''):
        """Create a new PatientDB inside an existing PatientDB class"""
        return PatientDB(name=name)

    def load(self, path):
        print(f"Loading PatientDB from {path}")
        with open(path, 'r') as f:
            for line in f:
                patient = json.loads(line, cls=EntityDecoder)
                self.add_patient(patient, entity_id=str(patient.patient_id))

    def generate_path_with_time(self, path: str, extension: str) -> str:
        """Generate path string with time included."""
        timestr = time.strftime("%Y%m%d-%H%M%S")
        time_path = f"{path}_{timestr}.{extension}"

        return time_path

    def dump(self, output_dir: str, path: str, extension: str = "jsonl",
             unique: bool = False, must_have_events: bool = True):
        """Dump patients KG to a file."""
        if unique:
            path = self.generate_path_with_time(path, extension)
        output_path = f"{output_dir}/{path}"
        print(f"Dumping {len(self.patients)} patients to {output_path}")

        c: Counter = Counter()
        with open(output_path, 'w') as f:
            for key in sorted(self.data['patients'].keys(), key=int):
                c['num_keys'] += 1
                patient = self.get_patient_by_id(key)
                if not patient:
                    import pdb
                    pdb.set_trace()
                    continue
                try:
                    if must_have_events:
                        if patient.num_events() < 1:
                            c['skipped_no_events'] += 1
                            continue
                    patient_dump = json.dumps(patient, cls=EntityEncoder)
                    f.write(f"{patient_dump}\n")
                    c['successful_dumps'] += 1
                except TypeError as e:
                    c['failed_dumps'] += 1
                    print(f"e: {e}")
                    import pdb
                    pdb.set_trace()
                    #print(f"Failed to dump patient {key}")
        print(f"{c}")

    def num_entities(self, entity):
        return len(self.data[entity].keys())

    def num_patients(self):
        return self.num_entities('patients')

    def num_visits(self):
        return self.num_entities('visits')

    def num_events(self):
        return self.num_entities('events')

    def num_visits_iter(self):
        num_visits = 0
        for patient in self.patients:
            num_visits += patient.num_visits()
        return num_visits

    # FIXME
    def num_events_iter(self):
        num_events = 0
        for patient in self.patients:
            num_events += patient.num_events()
        return num_events

    def get_events(self, event_type):
        matched_events = []
        for event in self.events:
            if event.event_type == event_type:
                matched_events.append(event)
        return matched_events

    def gender_counts(self):
        gender_counter = Counter()
        for patient in self.patients:
            gender_counter[patient.gender] += 1
        return gender_counter

    def get_patient_ids(self):
        patient_ids = set()
        for patient_id in self.data['patients'].keys():
            patient_ids.add(patient_id)
        patient_ids = list(patient_ids)
        return patient_ids

    def generate_patients_from_ids(self, patient_ids):
        """Generate patients from list of IDs."""

        for patient_id in patient_ids:
            patient = Patient(patient_id=str(patient_id))
            self.add_patient(patient, entity_id=str(patient_id))

    def merge_patient(self, patient_orig):
        patient_id = patient_orig.patient_id
        patient = self.get_patient_by_patient_id(patient_id)
        if patient:
            entity_id = patient.entity_id
            # print(f"Overwriting patient {patient_id} "
            #      f"w/ entity_id: {entity_id}")
        else:
            entity_id = None
        self.add_patient(patient_orig, entity_id=str(entity_id))

    def max_entity_key(self, entity):
        events_keys = self.data[entity].keys()
        events_keys = [int(x) for x in events_keys]
        max_events_key = 0
        if events_keys:
            max_events_key = max(events_keys)
        # FIXME, make sure this is correct
        #max_events_key = str(max_events_key)
        return max_events_key

    def find_empty_entity_key(self, entity) -> str:
        max_entity_key = self.max_entity_key(entity)
        full_entity_keys = set()
        for key in range(max_entity_key):
            full_entity_keys.add(key)
        actual_entity_keys = set(self.data[entity].keys())
        available_entity_keys = full_entity_keys.difference(actual_entity_keys)

        if not available_entity_keys:
            # Adding new key to full dict
            events_key = self.num_entities(entity)
        else:
            events_key = random.choice(list(available_entity_keys))

        return events_key

    def get_random_patient(self):
        random_patient = random.choice(list(self.data['patients'].keys()))
        return random_patient

    def print_random_patient(self):
        random_patient = self.get_random_patient()
        print(random_patient)

    def add_event(self, event: Event, entity_id: str = None):
        if not entity_id:
            # FIXME
            #entity_id = self.find_empty_entity_key('events')
            entity_id = self.num_entities('events')
        event.entity_id = entity_id
        self.data['events'][entity_id] = event
        return event

    def add_visit(self, visit: Visit, entity_id: str = None):
        if not entity_id:
            # FIXME, seems slow
            #entity_id = self.find_empty_entity_key('visits')
            entity_id = self.num_entities('visits')
        visit.entity_id = entity_id
        added_events = []
        # Add events nested in visit
        for event in visit.events:
            added_event = self.add_event(event)
            added_events.append(added_event)
        visit.events = added_events
        self.data['visits'][entity_id] = visit
        return visit

    def add_patient(self, patient: Patient, entity_id: str = None):
        if not entity_id:
            #entity_id = self.find_empty_entity_key('patients')
            entity_id = self.num_entities('patients')
        patient.entity_id = entity_id
        entity_id_patient = self.data['patients'].get(entity_id)
        if entity_id_patient:
            # print(f'Overwriting patient {entity_id_patient.patient_id}
            # w/ entity_id {entity_id}')
            pass
        added_visits = []
        # Add visits nested in patient
        for visit in patient.visits:
            added_visit = self.add_visit(visit)
            added_visits.append(added_visit)
        patient.visits = added_visits
        self.data['patients'][entity_id] = patient
        return patient

    # FIXME

    def match_patients(self, name, term, event_type_roles=None):
        if not event_type_roles:
            print('You must provide event_type_roles for match_patients().'
                  'Exiting...')
            sys.exit(1)

        matches = set()
        # Search all patients
        for patient in self.patients:
            patient_id = patient.patient_id
            # Search all patient visits
            for visit in patient.visits:
                visit_id = visit.visit_id
                # Search all patient events
                for event in visit.events:
                    event_id = event.event_id
                    event_type = event.event_type
                    # No event type, skipping. This shouldn't happen
                    if not event_type:
                        continue
                    event_roles = event_type_roles.get(event_type)
                    # No event roles to check for this event type
                    if not event_roles:
                        continue
                    for role in event_roles:
                        try:
                            compare_term = event.roles[role]
                        except KeyError:
                            # Continue to next key if this event doesn't have
                            # this role
                            continue
                        # FIXME, is this wise?
                        compare_term = compare_term.lower()
                        if term == compare_term:
                            #print(f"Matched patient_id: {patient_id}")
                            match = Match(patient_id, visit_id, event_id,
                                          event_type, role, term)
                            matches.add(match)

        return matches

    def attach_visits_to_patients(self, patient_ids):
        c = Counter()
        patient_ids = [str(x) for x in patient_ids]
        num_visits = len(self.visits)
        for i, visit in enumerate(self.visits):
            if i % 100000 == 0:
                print(f"{now_str()} visit {i}/{num_visits}")
            patient_id = str(visit.patient_id)
            # Skipping patient_ids not in patient_ids set
            if patient_id not in patient_ids:
                continue
            # FIXME
            #patient = self.get_patient_by_patient_id(patient_id)
            patient = self.data['patients'][patient_id]
            if not patient:
                import pdb
                pdb.set_trace()
                c['missing'] += 1
                continue
            c['success'] += 1
            patient.visits.append(visit)
        print(f"Vists, Num missing keys: {c['missing']}\n"
              f"Visits, Num successful keys: {c['success']}")

    def merge_patients(self, patients):
        #print(f"Merging patient DBs...")
        # TODO: make graceful
        for patient in patients.patients:
            self.merge_patient(patient)

    def find_patient_by_patient_id(self, patient_id: str):
        p = None
        for patient in self.patients:
            if patient.patient_id == patient_id:
                # We have found a patient
                p = patient
                break
        return p

    def add_demographic_info(self, demographics, use_dask):
        print("Adding demographic info")
        c = Counter()
        c['success_add_demographics'] = 0
        c['fail_patients_not_found'] = 0
        for row in demographics.itertuples():
            person_id = row.person_id
            person_id_key = str(person_id)
            # Does this person exist in the patient DB already?
            patient = self.data['patients'].get(person_id_key)
            if not patient:
                #import pdb;pdb.set_trace()
                #print(f"Not finding {person_id_key} in patients PatientDB")
                c['fail_patients_not_found'] += 1
                continue

            c['success_add_demographics'] += 1
            patient.date_of_birth = date(
                row.year_of_birth, row.month_of_birth, row.day_of_birth)
            patient.gender = row.gender
            patient.race = row.race
            patient.ethnicity = row.ethnicity
        print(f"{c}")

    def calculate_patient_ages(self, compare_date):
        max_age = 0
        min_age = 0
        for patient in self.patients:
            # We can't calculate patient ages w/o dob

            if not patient.date_of_birth:
                # FIXME
                patient.age = -1
                min_age = -1
                continue
            dob = patient.date_of_birth
            age = compare_date - dob
            # Full years old, birthdays essentially
            age_years = int(age.days / 365)
            #import pdb;pdb.set_trace()
            patient.age = age_years  # FIXME, best way?
            if patient.age > max_age:
                max_age = patient.age
            if patient.age < min_age:
                min_age = patient.age

            # Set patient.adult attribute based off age
            if patient.age >= 18:
                patient.adult = True
            elif patient.age < 0:
                print(f"Found patient with negative age: {patient.age}")
                import pdb
                pdb.set_trace()
            else:
                patient.adult = False
        return min_age, max_age

    def calulcate_patient_is_adult(self):
        for patient in self.patients:
            if not patient.age:
                continue
            if patient.age >= 18:
                patient.adult = True
            else:
                patient.adult = False

    def calculate_age_gender_distribution(self, min_age, max_age, genders,
                                          path):
        print("Calculating age distribution...")
        success_counter = Counter()
        age_counter = Counter()
        age_range = range(min_age, max_age + 1)
        ages = dict()
        #ages['all'] = []
        # for gender in genders:
        #    ages[gender] = []
        for patient in self.patients:
            success_counter['total_patients'] += 1
            if not patient.age:
                success_counter['patients_without_age'] += 1
                continue
            else:
                success_counter['patients_with_age'] += 1
            age_counter[str(patient.age)] += 1
            # ages['all'].append(patient.age)
            if not patient.gender:
                success_counter['patients_without_gender'] += 1
                continue
            else:
                success_counter['patients_with_gender'] += 1
            if not ages.get(patient.gender):
                ages[str(patient.gender)] = []
            ages[str(patient.gender)].append(patient.age)
        print(f"age_counter: {age_counter}")
        print(f"success_counter: {success_counter}")
        ages_bins = list(age_range)
        sorted_gender_keys = sorted(ages.keys())
        np_ages = dict()

        for gender in sorted_gender_keys:
            np_ages[str(gender)] = dict()
            np_ages[str(gender)] = np.array(ages[str(gender)])

        import pdb
        pdb.set_trace()
        self.plot_age_gender_distribution(path, np_ages, ages_bins,
                                          sorted_gender_keys)

    def get_age_colors_legend(self, ages):
        r_ages = []
        avail_colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k', 'w']
        colors = []
        legend = dict()
        color_index = 0
        for gender, gender_ages in ages.items():
            r_ages.append(gender_ages)
            color = avail_colors[color_index]
            colors.append(color)
            color_index += 1
            legend[gender] = color
        return r_ages, colors, legend

    def get_n_colors(self, n):
        #base_colors = ['b', 'g', 'r', 'c', 'm', 'y']
        tableau_colors = ['tab:blue', 'tab:orange', 'tab:green', 'tab:red',
                          'tab:purple', 'tab:brown', 'tab:pink', 'tab:gray',
                          'tab:olive', 'tab:cyan']
        avail_colors = tableau_colors
        colors = []
        for i in range(n):
            avail_color = avail_colors[i]
            colors.append(avail_color)
        if len(colors) != n:
            import pdb
            pdb.set_trace()
            print('Not generating correct amount of colors')
        return colors

    def plot_age_gender_distribution(self, path, ages, ages_bins, legend):
        print("Plotting age distribution...")
        fig, ax = plt.subplots()
        # FIXME for generalized genders
        #age_arrays, colors, labels = self.get_age_colors_legend(ages)
        gender_keys = sorted(ages.keys())
        n_genders = len(gender_keys)
        colors = self.get_n_colors(n_genders)
        legend = dict()
        values = list()
        for color_i, gender in enumerate(gender_keys):
            legend[gender] = colors[color_i]
            values.append(ages[gender])
        import pdb
        pdb.set_trace()
        ax.hist(
            values,
            bins=ages_bins,
            density=True,
            histtype='bar',
            stacked=True,
            color=colors)
        ax.legend(legend)
        ax.set_xlabel('Age(Years)', fontsize=16)
        ax.set_ylabel('Count', fontsize=16)
        print(f"Saving age histogram to {path}")
        fig.savefig(path)

    def get_stats(self):
        total_num_visits = 0
        for patient_id, patient in self.data['patients'].items():
            num_visits = len(patient.visits)
            total_num_visits += num_visits
        stats = dict()
        stats['avg_num_visits'] = total_num_visits / float(len(self.patients))
        return stats

    def get_event_roles(self, event_types, meddra_roles=False):
        event_roles = set()

        if meddra_roles:
            meddra_roles = ['SOC_text', 'HLGT_text',
                            'HLT_text', 'PT_text', 'concept_text']
            # 'HLGT_CUI', 'HLT_CUI', 'PT_CUI', 'SOC_CUI', 'medID',
            # 'PExperiencer', 'HLGT', 'HLT', 'PT', 'SOC']
            event_roles.update(meddra_roles)

        for event_type in event_types:
            if event_type == 'DiagnosisEvent':
                event_type_roles = ['diagnosis_icd9',
                                    'diagnosis_name', 'diagnosis_long_name']
            elif event_type == 'LabEvent':
                event_type_roles = ['test_name', 'test_status', 'test_value']
            elif event_type == 'MedicationEvent':
                event_type_roles = ['dosage', 'duration',
                                    'indication', 'medication']
            elif event_type == 'PatientEvent':
                pass  # TODO event_type_roles= ['attribute', 'attribute_value']
            elif event_type == 'ProcedureEvent':
                event_type_roles = ['procedure_icd9',
                                    'procedure_name', 'targeted_organs']
            elif event_type == 'VitalEvent':
                event_type_roles = ['location', 'vital_outcome']

            event_roles.update(event_type_roles)
        return event_roles

    def get_event_counters(self, event_types, meddra_roles=False):
        counters = dict()
        items = dict()

        entity_levels = ['patient', 'visit', 'event']
        event_roles = self.get_event_roles(event_types, meddra_roles=True)

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

        # Point out that items sets are temp

        # Iterate through all patients
        for patient in self.patients:
            # Clear patient items set
            for role in event_roles:
                patient_items[role].clear()
            # Iterate through all patient visits
            for visit in patient.visits:
                # Clear visit items set
                for role in event_roles:
                    visit_items[role].clear()
                # Iterate through all visit events
                for event in visit.events:
                    if event.event_type not in event_types:
                        continue
                    # Clear event items
                    for role in event_roles:
                        event_items[role].clear()

                    # Add items
                    for role in event_roles:
                        # Skip roles that don't exist in event
                        if not event.roles.get(role):
                            continue
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

        return counters, event_roles, entity_levels

    def get_event_counters_from_matches(self, matches,
                                        event_type_roles,
                                        cnt_event_type_roles,
                                        entity_levels=None,
                                        patient_visits=None,
                                        patient_ids=None):
        if not entity_levels:
            entity_levels = ['patient', 'visit', 'event']

        if patient_visits:
            patient_ids = patient_visits.keys()
        else:
            patient_ids = self.get_patient_ids()

        if not patient_ids:
            print("You must provide some form of patient ids. Exiting...")
            sys.exit(1)

        counters = dict()
        items = dict()

        # Prepare counter and event_items
        for entity_level in entity_levels:
            counters[entity_level] = dict()
            items[entity_level] = dict()
            for event_type, event_roles in cnt_event_type_roles.items():
                # Create event type dicts
                if not counters[entity_level].get(event_type):
                    counters[entity_level][event_type] = dict()
                if not items[entity_level].get(event_type):
                    items[entity_level][event_type] = dict()

                for event_role in event_roles:
                    counters[entity_level][event_type][event_role] = Counter()
                    items[entity_level][event_type][event_role] = set()

        debug = False

        for patient_id in patient_ids:
            #print(f"patient_id: {patient_id}")
            patient = self.get_patient_by_id(patient_id)
            if patient_visits:
                visit_ids = patient_visits[patient_id].keys()
            else:
                visit_ids = patient.get_visit_ids()
            # clear patient item counters
            for event_type, event_roles in cnt_event_type_roles.items():
                for role in event_roles:
                    items['patient'][event_type][role].clear()

            for visit_id in visit_ids:
                if debug:
                    print(f"visit_id: {visit_id}")
                visit = patient.get_visit_by_id(visit_id)
                # clear visit item counters
                for event_type, event_roles in cnt_event_type_roles.items():
                    for role in event_roles:
                        items['visit'][event_type][role].clear()
                if debug:
                    print("Done clearing visit items")

                for event in visit.events:
                    for event_type, event_roles in cnt_event_type_roles.items():
                        for role in event_roles:
                            items['event'][event_type][role].clear()
                    if debug:
                        print("Done clearing event items")
                    if event.event_type not in cnt_event_type_roles.keys():
                        continue
                    event_roles = cnt_event_type_roles[event.event_type]
                    for event_role in event_roles:
                        item = event.roles[event_role]
                        # Add item to all item sets
                        for entity_level in entity_levels:
                            items[entity_level][event.event_type][event_role].add(
                                item)

                    # Count event items
                    for event_type, event_roles in cnt_event_type_roles.items():
                        for event_role in event_roles:
                            for item in items['event'][event_type][event_role]:
                                counters['event'][event_type][event_role][item] += 1
                # Count visit items
                for event_type, event_roles in cnt_event_type_roles.items():
                    for event_role in event_roles:
                        for item in items['visit'][event_type][event_role]:
                            counters['visit'][event_type][event_role][item] += 1
            # Count patient items
            for event_type, event_roles in cnt_event_type_roles.items():
                for event_role in event_roles:
                    for item in items['patient'][event_type][event_role]:
                        counters['patient'][event_type][event_role][item] += 1
        return counters

    def get_visit_dates(self, time_freq='M'):
        visit_dates = set()
        for patient_id, patient in self.data['patients'].items():
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

        # start_visit_date = \
        # datetime(min_visit_date.year, min_visit_date.month, 1)
        # end_visit_date = \
        # datetime(max_visit_date.year, max_visit_date.month, 1)
        #delta = timedelta()
        # FIXME
        # for visit_date in rrule.rrule(rrule.MONTHLY,
        # dtstart=start_visit_date,
        # until=end_visit_date):
        #    print(visit_date)
        #import pdb;pdb.set_trace()
        return visit_dates

    def select_date(self, name, year=None, month=None, day=None):
        match_year = bool(year)
        match_month = bool(month)
        match_day = bool(day)
        date_db = self.reproduce(name=name)
        for patient_id, patient in self.data['patients'].items():
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
                date_db.add_patient(matched_patient, entity_id=str(
                    matched_patient.patient_id))
        return date_db

    def get_unique_genders(self):
        unique_genders = set()
        for patient in self.patients:
            unique_genders.add(patient.gender)
        return unique_genders

    def get_count_event_types(self):
        unique_event_types = Counter()
        for event in self.events:
            unique_event_types[event.event_type] += 1
        return unique_event_types

    def get_unique_ethnicities(self):
        unique_ethnicities = set()
        for patient in self.patients:
            unique_ethnicities.add(patient.ethnicity)
        return unique_ethnicities

    def get_unique_races(self):
        unique_races = set()
        for patient in self.patients:
            unique_races.add(patient.race)
        return unique_races

    def agg_time(self, time_freq='M'):
        # Split patient DB in a DB per each time freq
        visit_dates = self.get_visit_dates(time_freq)
        visit_date_dbs = dict()
        # freq_aggs = self.agg_key(entity_level='Visit', '')
        # iterate through visit dates and create a patientDB per time step
        for visit_date in visit_dates:
            date_key = visit_date.strftime("%Y-%m")
            visit_date_db = self.select_date(name=date_key,
                                             year=visit_date.year,
                                             month=visit_date.month)
            visit_date_dbs[date_key] = visit_date_db

        #import pdb;pdb.set_trace()
        return visit_date_dbs

    def agg_ethnicity(self):
        unique_ethnicities = self.get_unique_ethnicities()
        ethnicity_dbs = dict()
        # Create empty ethnicity dbs
        for ethnicity in unique_ethnicities:
            ethnicity_dbs[ethnicity] = self.reproduce(name=ethnicity)
        # Put patients in their respective ethnicity dbs
        for patient in self.patients:
            ethnicity_dbs[patient.ethnicity].add_patient(
                patient, entity_id=str(patient.patient_id))

        import pdb
        pdb.set_trace()
        return ethnicity_dbs

    def agg_gender(self):
        unique_genders = self.get_unique_genders()
        gender_dbs = dict()
        # Create empty gender dbs
        for gender in unique_genders:
            gender_dbs[gender] = self.reproduce(name=gender)
        # Put patients in their respective gender dbs
        for patient in self.patients:
            gender_dbs[patient.gender].merge_patient(patient)
        import pdb
        pdb.set_trace()
        return gender_dbs

    def agg_race(self):
        unique_races = self.get_unique_races()
        race_dbs = dict()
        # Create empty race dbs
        for race in unique_races:
            race_dbs[race] = self.reproduce(name=race)
        # Put patients in their respective race dbs
        for patient in self.patients:
            race_dbs[patient.race].merge_patient(patient)
        import pdb
        pdb.set_trace()
        return race_dbs

    def get_event_by_event_id(self,
                              patient_id: str,
                              event_id: str) -> Optional[Any]:
        e = None
        for event in self.events:
            # We have found an event matching our criteria
            if event.event_id == event_id:
                e = event
                # breaking at the first visit
                break
        return e

    def get_visit_by_visit_id(self,
                              patient_id: str,
                              visit_id: str) -> Optional[Any]:
        v = None
        for visit in self.visits:
            # We have found a vist matching our criteria
            if visit.visit_id == visit_id and visit.patient_id == patient_id:
                v = visit
                # breaking at the first visit
                break
        return v

    def get_patient_by_id(self, patient_id: str) -> Optional[Any]:
        patient = self.data['patients'].get(patient_id)
        return patient

    def attach_events_to_visits(self):
        c = Counter()

        # Attach events to visits
        for i, event in enumerate(self.events):
            if i % 10000 == 0:
                now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print(f"{now_str} Attaching event {i} out of {self.num_events()}")
            try:
                # FIXME, we dont have unique_visit_ids
                patient = self.get_patient_by_id(event.patient_id)
                if not patient:
                    import pdb
                    pdb.set_trace()
                    print("Couldn't find patient")
                visit = patient.get_visit_by_id(event.visit_id)
                # FIXME, is this necessary?
                # FIXME, choosing to create visits here instead of creating all possible
                if not visit:
                    #import pdb;pdb.set_trace()
                    #print("Couldn't find visit.")
                    date_str = event.visit_id
                    date_obj = date_str_to_obj(date_str)
                    visit = Visit(
                        date=date_obj, visit_id=event.visit_id, patient_id=event.patient_id)
                    visit = self.add_visit(visit)
                    patient.visits.append(visit)
                visit.events.append(event)
                c['successful_keys'] += 1
            except KeyError:
                import pdb
                pdb.set_trace()
                c['missing_keys'] += 1
        print(f"Events, Num missing keys: {c['missing_keys']}\n"
              f"Events, Num successful keys: {c['successful_keys']}")

    def select_non_empty_patients(self, patient_ids: Set[str]) -> Set[int]:
        """Filter out patients IDs with no events."""
        non_empty_patient_ids = set()
        for event in self.events:
            patient_id = int(event.patient_id)
            non_empty_patient_ids.add(patient_id)

        # FIXME, is this necessary?
        if not non_empty_patient_ids.issubset(patient_ids):
            print("Non-empty patient IDs are not a subset of patient IDs")
            import pdb
            pdb.set_trace()
        print(f"Before filtering out empty patients: {len(patient_ids)} "
              f"patient IDs")
        print(f"After filtering out empty patients: "
              f"{len(non_empty_patient_ids)} patient IDs")

        return non_empty_patient_ids

    def match_terms(self, terms, event_type_roles):
        print(f"Matching terms:\n\t{terms}\n")
        print(f"Matching against:\n\t{self}\n")
        #patients_matched = self.reproduce(name='patients_matched')
        matches = set()
        for term in terms:
            # FIXME
            # print(f"{term}")
            term_matches = self.match_patients(
                f'{term}_patients_matched',
                term,
                event_type_roles=event_type_roles)
            #import pdb;pdb.set_trace()
            matches = matches.union(term_matches)
            unique_match_ids = get_unique_match_ids(matches)
            num_term_patients_matched = len(unique_match_ids['patient'])
            print(
                f"term: {term}, num_matches: {len(term_matches)}, num_term_patients_matched: {num_term_patients_matched}")
        return matches

    def generate_from_matches(self, matches, name=''):
        patients_matched = self.reproduce(name=name)
        unique_match_ids = get_unique_match_ids(matches)
        patient_ids = unique_match_ids['patient'].keys()
        for patient_id in patient_ids:
            patient = self.patients[patient_id]
            patients_matched.merge_patient(patient)
        return patients_matched
