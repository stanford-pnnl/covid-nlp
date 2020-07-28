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


def main():
    # Setup variables
    #path = 'clever_output_test.parquet'
    #path = 'data/prototype_5k_notes2.hdf'
    data_dir = 'data'
    meddra_extractions_dir = f"{data_dir}/medDRA_extractions"
    meddra_extractions_path = f"{meddra_extractions_dir}/meddra_hier_batch3.hdf"

    # get batch #3 of medDRA extractions from file
    meddra_extractions = get_df(meddra_extractions_path)

    # Get distinct Patient ID values from dataframe
    patient_ids = get_patient_ids(meddra_extractions, 'patid')
   
    # Generate patients from IDs
    patients = generate_patients_from_ids(patient_ids)

    # Sort patient keys
    sorted_patient_keys = sorted(patients.keys(), key=int)

    # Generate patient dump path
    patients_dump_path = generate_path_with_time('output/patients_dump', 'jsonl')

    # Add empty visits to patients
    # Get all the note ids and then build visits based on that
    for row in meddra_extractions.itertuples():
        patient = patients[row.patid]
        extraction_date = row.date
        visit_found = False
        visit = None
        # Check if patient has visit with date
        for patient_visit in patient.visits:
            patient_visit_date = patient_visit.hadm_id
            if extraction_date == patient_visit_date:
                visit_found = True
                break

        # Create visit if not found
        if not visit_found:
            visit = Visit(hadm_id=extraction_date)
            patient.visits.append(visit)

    num_patients = len(patients)
    sum_visits = 0
    min_visits = 9999
    max_visits = 0
    for patient in patients.values():
        num_visits = len(patient.visits)
        sum_visits += num_visits
        if min_visits > num_visits:
            min_visits = num_visits
        if max_visits < num_visits:
            max_visits = num_visits

    avg_visits = sum_visits / float(num_patients)

    print(f"Average visits per patient: {avg_visits}, max visits per patient: {max_visits}, min visits per patient: {min_visits}")

    # Dump patients
    export_patients(patients_dump_path, patients, sorted_patient_keys)

    import pdb;pdb.set_trace()
    print()


if __name__ == '__main__':
    main()
