import datetime
import os
import sys
from collections import Counter
from tqdm import tqdm
import matplotlib.cbook as cbook
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from data_schema import EntityEncoder, Event, Patient, Visit
from typing import Dict, Set, Any, List
import time
import json

def get_df(path):
    if '.parquet' in path:
        df = pd.read_parquet(path, engine='pyarrow')
    elif '.hdf' in path:
        df = pd.read_hdf(path)
    else:
        print(f"Unhandled path, no matching file extension: {path}")
        sys.exit(1)
    print(f"Successfully read dataframe from path: {path}")
    return df


def get_patient_ids(df, key):
    n_patient_ids = df[key].nunique()
    patient_ids = df[key].unique().tolist()
    print(f"Found {n_patient_ids} patient IDs")
    return patient_ids

def generate_patients_from_ids(patient_ids: Set[str]) -> Dict:
    """Generate patients from list of IDs."""
    patients: Dict = {}
    with tqdm(total=len(patient_ids), desc="Generating Patients from IDs")\
            as pbar:
        for patient_id in patient_ids:
            patients[patient_id] = Patient(patient_id=patient_id)
            pbar.update(1)

    return patients

def generate_path_with_time(path: str, extension: str) -> str:
    """Generate path string with time included."""
    timestr = time.strftime("%Y%m%d-%H%M%S")
    time_path = f"{path}_{timestr}.{extension}"

    return time_path


def export_patients(path: str, patients: Dict[str, Any],
                    sorted_keys: List[str] = None):
    """Export patients KG to a file."""
    print(f"Dumping {len(patients.values())} patients to {path}")

    # If sorted keys are not provided, come up with iteration values
    if not sorted_keys:
        keys = patients.keys()
    else:
        keys = sorted_keys

    c = Counter()
    c['num_keys'] = 0
    c['successful_dumps'] = 0
    c['failed_dumps'] = 0

    with open(path, 'w') as f:
        for key in keys:
            c['num_keys'] += 1
            patient = patients[key]
            try:
                patient_str = json.dumps(patient, cls=EntityEncoder)
                f.write(f"{patient_str}\n")
                c['successful_dumps'] += 1
            except TypeError as e:
                c['failed_dumps'] += 1
                print(f"e: {e}")
                import pdb;pdb.set_trace()
                #print(f"Failed to dump patient {key}")
    print(f"{c}")


def get_distinct_column_values(df, output_dir, keys):
    print("Getting distinct values from columns and dumping to files")
    for key in sorted(keys):
        try:
            distinct_column_values = df[key].unique().tolist()
        except KeyError:
            print(f"\t{key}, not in dataframe, skipping...")
            continue
        except TypeError:
            print(f"\t{key}, TypeError, skipping...")
            continue

        output_path = f"{output_dir}/{key}.txt"
        print(f"\t{key}, dumping {len(distinct_column_values)} distinct values to {output_path}")
        with open(output_path, 'w') as f:
            try:
                distinct_column_values = sorted(distinct_column_values)
            except TypeError:
                pass
            for distinct_column_value in distinct_column_values:
                f.write(f"{distinct_column_value}\n")


def get_diagnosis_events_depression(events, event_cnt, date, concept_text, PT_text):
    concept_text_depression = [
            'Anxious depression',
            'Bipolar depression',
            'Chronic depression',
            'Major depression',
            'Post stroke depression',
            'Postpartum depression',
            'Reactive depression',
            'ST segment depression',
            'bipolar depression',
            'chronic depression',
            'depression',
            'depression nos',
            'major depression',
            'manic depression',
            'mood depression',
            'post stroke depression',
            'postpartum depression',
            'reactive depression', 
            'suicidal depression']

    PT_text_depression = [
            'Major depression',
            'Perinatal depression',
            'Post stroke depression']

    # Check for 'depression'
    depression_diagnosis_event_found = False
    if concept_text in concept_text_depression:
        depression_diagnosis_event_found = True
    if PT_text in PT_text_depression:
        depression_diagnosis_event_found = True
    
    # If we find a depression diagnosis event, create it
    if depression_diagnosis_event_found:
        event_cnt['id'] += 1
        event_cnt['diagnosis'] += 1
        event_cnt['diagnosis_depression'] += 1
        depression_diagnosis_event = Event(chartdate=date, provenance=date, event_id=event_cnt['id'])
        depression_diagnosis_event.diagnosis_role(diagnosis_icd9='', diagnosis_name='Depression')
        depression_diagnosis_event.roles['concept_text'] = concept_text
        depression_diagnosis_event.roles['PT_text'] = PT_text
        events[str(event_cnt['id'])] = depression_diagnosis_event


def get_diagnosis_events_anxiety(events, event_cnt, date, concept_text, PT_text):
    concept_text_anxiety = [
            'Adjustment disorder with anxiety',
            'Chronic anxiety',
            'Generalized anxiety disorder',
            'Situational anxiety',
            'Social anxiety disorder',
            'adjustment disorder with anxiety',
            'anxiety',
            'anxiety attack',
            'anxiety disorder',
            'anxiety symptoms',
            'chronic anxiety',
            'generalized anxiety disorder',
            'separation anxiety',
            'situational anxiety',
            'social anxiety disorder']

    PT_text_anxiety = [
            'Adjustment disorder with anxiety',
            'Generalised anxiety disorder',
            'Illness anxiety disorder',
            'Separation anxiety disorder',
            'Social anxiety disorder']

    # Check for anxiety
    anxiety_diagnosis_event_found = False
    if concept_text in concept_text_anxiety:
        anxiety_diagnosis_event_found = True
    if PT_text in PT_text_anxiety:
        anxiety_diagnosis_event_found = True


    # If we find an anxiety diagnosis event, create it
    if anxiety_diagnosis_event_found:
        event_cnt['id'] += 1
        event_cnt['diagnosis'] += 1
        event_cnt['diagnosis_anxiety'] += 1
        anxiety_diagnosis_event = Event(chartdate=date, provenance=date, event_id=event_cnt['id'])
        anxiety_diagnosis_event.diagnosis_role(diagnosis_icd9='', diagnosis_name='Anxiety')
        anxiety_diagnosis_event.roles['concept_text'] = concept_text
        anxiety_diagnosis_event.roles['PT_text'] = PT_text
        events[str(event_cnt['id'])] = anxiety_diagnosis_event
 

def get_diagnosis_events_insomnia(events, event_cnt, date, concept_text, PT_text):
    concept_text_insomnia = [
            'Behavorial insomnia of childhood'
            'Chronic insomnia',
            'Initial insomnia',
            'Primary insomnia',
            'chronic insomnia',
            'insomnia', 
            'primary insomnia',
            'psychological insomnia']

    PT_text_insomnia = [
            'Behavioural insomnia of childhood',
            'Initial insomnia',
            'Middle insomnia',
            'Psychophysiologi insomnia',
            'Terminal insomnia']

    # Check for insomnia
    insomnia_diagnosis_event_found = False
    if concept_text in concept_text_insomnia:
        insomnia_diagnosis_event_found = True
    if PT_text in PT_text_insomnia:
        insomnia_diagnosis_event_found = True

    # If we find an insomnia diagnosis event, create it 
    if insomnia_diagnosis_event_found:
        event_cnt['id'] += 1
        event_cnt['diagnosis'] += 1
        event_cnt['diagnosis_insomnia'] += 1
        insomnia_diagnosis_event = Event(chartdate=date, provenance=date, event_id=event_cnt['id'])
        insomnia_diagnosis_event.diagnosis_role(diagnosis_icd9='', diagnosis_name='Insomnia')
        insomnia_diagnosis_event.roles['concept_text'] = concept_text
        insomnia_diagnosis_event.roles['PT_text'] = PT_text
        events[str(event_cnt['id'])] = insomnia_diagnosis_event


def get_diagnosis_events_distress(events, event_cnt, date, concept_text, PT_text):
    concept_text_distress = []
    PT_text_distress = []

    # Check for distress
    distress_diagnosis_event_found = False
    if concept_text in concept_text_distress:
        distress_diagnosis_event_found = True
    if PT_text in PT_text_insomnia:
        distress_diagnosis_event_found = True

    # If we find a distress diagnosis event, create it 
    if distress_diagnosis_event_found:
        event_cnt['id'] += 1
        event_cnt['diagnosis'] += 1
        event_cnt['diagnosis_distress'] += 1
        distress_diagnosis_event = Event(chartdate=date, provenance=date, event_id=event_cnt['id'])
        distress_diagnosis_event.diagnosis_role(diagnosis_icd9='', diagnosis_name='Distress')
        distress_diagnosis_event.roles['concept_text'] = concept_text
        distress_diagnosis_event.roles['PT_text'] = PT_text
        events[str(event_cnt['id'])] = distress_diagnosis_event


def get_diagnosis_events(events, event_cnt, df):
    columns = dict()
    for column in df.columns.tolist():
        columns[column] = Counter()
    print(df.head())

    for row in df.itertuples():
        #import pdb;pdb.set_trace()
        date = row.date

        HLGT_text = row.HLGT_text
        PT_text = row.PT_text
        concept_text = row.concept_text
   
        columns['HLGT_text'][HLGT_text] += 1
        columns['PT_text'][PT_text] += 1
        columns['concept_text'][concept_text] += 1
        

        # Check for different types of diagnosis events
        get_diagnosis_events_depression(events, event_cnt, date, concept_text, PT_text)
        get_diagnosis_events_anxiety(events, event_cnt, date, concept_text, PT_text)
        get_diagnosis_events_insomnia(events, event_cnt, date, concept_text, PT_text)
        get_diagnosis_events_distress(events, event_cnt, date, concept_text, PT_text)

    #print(f"columns: {columns}")

def get_events(df):
    event_cnt = Counter()
    events = {}
    # Diagnosis Events
    get_diagnosis_events(events, event_cnt, df)
    print(f"Found {event_cnt['id']} total events")
    print(f"Found {event_cnt['diagnosis']} diagnosis events")
    return events


def get_patient_visits(df):
    patient_visits = {}
    for row in df.itertuples():
        patient_id = row.patid
        visit_id = row.date  #FIXME, currently using date as visit ID
        if not visit_id:
            continue  # skip if missing ID
        if not patient_visits.get(patient_id, None):
            patient_visits[patient_id] = set()
        patient_visits[patient_id].add(visit_id)
    return patient_visits


def create_visits(patient_visits: Dict[str, set]) -> Dict[str, Visit]:
    """Create visit objects."""
    visits: Dict[str, Visit] = {}
    for patient_id, visit_set in patient_visits.items():
        for hadm_id in visit_set:
            visits[hadm_id] = Visit(hadm_id=hadm_id, provenance=patient_id)
    return visits


def attach_events_to_visits(events: Dict[str, Event],
                            visits: Dict[str, Visit]):
    num_missing_keys = 0
    num_successful_keys = 0

    # Attach events to visits
    for event in events.values():
        try:
            visits[event.provenance].events.append(event)
            num_successful_keys += 1
        except KeyError:
            num_missing_keys += 1
    print(f"Events, Num missing keys: {num_missing_keys}\n"
          f"Events, Num successful keys: {num_successful_keys}")


def attach_visits_to_patients(visits: Dict[str, Visit],
                              patients: Dict[str, Patient]):
    num_missing_keys = 0
    num_successful_keys = 0
    # Attach visits to Patients
    for visit in visits.values():
        try:
            patients[visit.provenance].visits.append(visit)
            num_successful_keys += 1
        except KeyError:
            num_missing_keys += 1
    print(f"Vists, Num missing keys: {num_missing_keys}\n"
          f"Visits, Num successful keys: {num_successful_keys}")


def select_non_empty_patients(patient_ids: Set[str],
                              visits: Dict[str, Visit]) -> Set[str]:
    """Filter out patients IDs with no visits."""
    print(f"Before filtering out empty patients: {len(patient_ids)} patient IDs")

    non_empty_patient_ids = set()
    for visit in visits.values():
        patient_id = visit.provenance
        non_empty_patient_ids.add(patient_id)

    if not non_empty_patient_ids.issubset(patient_ids):
        print("Non-empty patient IDs are not a subset of patient IDs")

    print(f"After filtering out empty patients: {len(non_empty_patient_ids)} patient IDs")

    return non_empty_patient_ids


def main():
    # Setup variables
    output_dir = 'output'
    distinct_column_values_dir = f"{output_dir}/distinct_column_values"
    #data_dir = 'data'
    #meddra_extractions_dir = f"{data_dir}/medDRA_extractions"
    covid_data_dir = f"/share/pi/stamang/covid/data"
    notes_2019_2020_dir = f"{covid_data_dir}/notes_20190901_20200701/extracted_notes"
    notes_2018_2019_dir = f"{covid_data_dir}/notes_20180901_20190701/extracted_notes"
    # Currently just testing one file, should eventually iterate through all extracted files
    #meddra_extractions_path = f"{meddra_extractions_dir}/meddra_hier_batch3.hdf"
    meddra_extractions_path = f"{notes_2018_2019_dir}/extracted_notes_batch391.parquet"
    # Generate patient dump path
    patients_dump_path = generate_path_with_time(path='output/patients', extension='jsonl')


    # get batch #3 of medDRA extractions from file
    meddra_extractions = get_df(meddra_extractions_path)
    columns = sorted(meddra_extractions.columns.tolist())
    print(f"Dataframe column names:\n\t{columns}")
    # Dump distinct column values for debug
    column_keys = \
            ['note_title', 'concept_text', 'polarity', 'present', 
             'PT_text', 'HLT_text', 'HLGT_text', 'SOC_text']
    # Make sure output directories are created
    try:
        os.makedirs(distinct_column_values_dir)
    except OSError:
        pass

    get_distinct_column_values(meddra_extractions, distinct_column_values_dir, columns)

    # Get distinct Patient ID values from dataframe
    patient_ids = get_patient_ids(meddra_extractions, 'patid')
   
    events = get_events(meddra_extractions)
    if not events:
        print("Empty events dict! Exiting...")
        sys.exit(0)
    print(f"Found {len(events.values())} events values")

    # Get patient visits
    patient_visits = get_patient_visits(meddra_extractions)

    # Create visit objects
    visits = create_visits(patient_visits)

    # Attach events to visits
    attach_events_to_visits(events, visits)

    # Filter out patient IDs that don't have any visits
    patient_ids = select_non_empty_patients(patient_ids, visits)

    # Generate patients from IDs
    patients = generate_patients_from_ids(patient_ids)

    # Attach visits to patients
    attach_visits_to_patients(visits, patients)

    # Sort patient keys
    sorted_patient_keys = sorted(patients.keys(), key=int)

    #import pdb;pdb.set_trace()    

    # Dump patients
    export_patients(patients_dump_path, patients, sorted_patient_keys)


if __name__ == '__main__':
    main()
