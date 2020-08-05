import datetime
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

    with open(path, 'w') as f:
        for key in keys:
            patient = patients[key]
            patient_str = json.dumps(patient, cls=EntityEncoder)
            f.write(f"{patient_str}\n")


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


def get_diagnosis_events(events, event_cnt, df):
    PT_text_collection = Counter()
    for row in df.itertuples():
        # anxiety
        HLGT_text = row.HLGT_text
        PT_text = row.PT_text
        date = row.date
        try:
            if 'Anxiety disorders and symptoms' in HLGT_text:
                event_cnt['id'] += 1
                event_cnt['diagnosis'] += 1
                PT_text_collection[PT_text] += 1
                diagnosis_event = Event(chartdate=date, provenance=date, event_id=event_cnt['id'])
                diagnosis_event.diagnosis_role(diagnosis_icd9=PT_text, diagnosis_name='Anxiety disorder and symptoms')
                events[str(event_cnt['id'])] = diagnosis_event
        except TypeError:
            pass
    #PT_text_collection.sort()
    print(f"{PT_text_collection}")
    sorted_keys = PT_text_collection.keys()
    for k in sorted_keys:
        print(f"k{k}, count{PT_text_collection[k]}")

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
    data_dir = 'data'
    meddra_extractions_dir = f"{data_dir}/medDRA_extractions"
    meddra_extractions_path = f"{meddra_extractions_dir}/meddra_hier_batch3.hdf"
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

    # Dump patients
    export_patients(patients_dump_path, patients, sorted_patient_keys)

    import pdb;pdb.set_trace()
    print()


if __name__ == '__main__':
    main()
