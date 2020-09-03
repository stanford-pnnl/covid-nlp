import argparse
import glob
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Set

import dask
import dask.dataframe as dd
import matplotlib.cbook as cbook
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow
from tqdm import tqdm

from data_schema import EntityEncoder, Event, Patient, Visit


def get_df(path, use_dask):
    if use_dask:
        df = dd.read_parquet(path, engine='pyarrow')
    else:
        if '.parquet' in path:
            df = pd.read_parquet(path, engine='pyarrow')
        elif '.hdf' in path:
            df = pd.read_hdf(path)
        else:
            print(f"Unhandled path, no matching file extension: {path}")
            sys.exit(1)
    print(f"Successfully read dataframe from path: {path}")
    return df


def get_patient_ids(df, use_dask=False):
    if use_dask:
        unique_patient_ids = df.patid.unique().compute()
        nunique_patient_ids = df.patid.nunique().compute()
    else:
        unique_patient_ids = df.patid.unique()
        nunique_patient_ids = df.patid.unique()
    print(f"Found {nunique_patient_ids} patient IDs")
    return unique_patient_ids


def generate_patients_from_ids(patient_ids: Set[str]) -> Dict:
    """Generate patients from list of IDs."""
    patients: Dict = {}
    # FIXME
    # with tqdm(total=len(patient_ids), desc="Generating Patients from IDs")\
    #         as pbar:
    for patient_id in patient_ids:
        patients[str(patient_id)] = Patient(patient_id=str(patient_id))
        # pbar.update(1)

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

    c: Counter = Counter()
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
                import pdb
                pdb.set_trace()
                #print(f"Failed to dump patient {key}")
    print(f"{c}")


def get_distinct_column_values(df, output_dir, keys, use_dask=False):
    print("Getting distinct values from columns and dumping to files")
    for key in sorted(keys):
        try:
            distinct_column_values = list(df[key].unique())  # .tolist()
        except KeyError:
            print(f"\t{key}, not in dataframe, skipping...")
            continue
        except TypeError:
            print(f"\t{key}, TypeError, skipping...")
            continue
        except AttributeError:
            print(f"\t{key}, AttributeError, skipping...")
            continue
        except NotImplementedError:
            print(f"\t{key}, NotImplementedError, skipping...")
            continue

        output_path = f"{output_dir}/{key}.txt"
        print(f"\t{key}, dumping {len(distinct_column_values)} distinct values"
              f" to {output_path}")
        with open(output_path, 'w') as f:
            try:
                distinct_column_values = sorted(distinct_column_values)
            except TypeError:
                pass
            for distinct_column_value in distinct_column_values:
                f.write(f"{distinct_column_value}\n")


def get_diagnosis_events_depression(events, event_cnt, date, concept_text,
                                    PT_text, patient_id):
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
    if PT_text in PT_text_depression:
        depression_diagnosis_event_found = True
        diagnosis_long_name = PT_text
    if concept_text in concept_text_depression:
        depression_diagnosis_event_found = True
        diagnosis_long_name = concept_text

    # If we find a depression diagnosis event, create it
    if depression_diagnosis_event_found:
        event_cnt['id'] += 1
        event_cnt['diagnosis'] += 1
        event_cnt['diagnosis_depression'] += 1
        depression_diagnosis_event = Event(
            chartdate=date, provenance=date, event_id=event_cnt['id'],
            patient_id=patient_id)
        depression_diagnosis_event.diagnosis_role(
            diagnosis_icd9='', diagnosis_name='Depression',
            diagnosis_long_name=diagnosis_long_name)
        depression_diagnosis_event.roles['concept_text'] = concept_text
        depression_diagnosis_event.roles['PT_text'] = PT_text
        events[str(event_cnt['id'])] = depression_diagnosis_event


def get_diagnosis_events_anxiety(events, event_cnt, date, concept_text,
                                 PT_text, patient_id):
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
    if PT_text in PT_text_anxiety:
        anxiety_diagnosis_event_found = True
        diagnosis_long_name = PT_text
    if concept_text in concept_text_anxiety:
        anxiety_diagnosis_event_found = True
        diagnosis_long_name = concept_text

    # If we find an anxiety diagnosis event, create it
    if anxiety_diagnosis_event_found:
        event_cnt['id'] += 1
        event_cnt['diagnosis'] += 1
        event_cnt['diagnosis_anxiety'] += 1
        anxiety_diagnosis_event = Event(
            chartdate=date, provenance=date, event_id=event_cnt['id'],
            patient_id=patient_id)
        anxiety_diagnosis_event.diagnosis_role(
            diagnosis_icd9='', diagnosis_name='Anxiety',
            diagnosis_long_name=diagnosis_long_name)
        anxiety_diagnosis_event.roles['concept_text'] = concept_text
        anxiety_diagnosis_event.roles['PT_text'] = PT_text
        events[str(event_cnt['id'])] = anxiety_diagnosis_event

# TODO match with ability to define meddra level matches


def get_diagnosis_events_insomnia(events, event_cnt, date, concept_text,
                                  PT_text, patient_id):
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

    #Match(SOC="*", HLGT="cardiac_valve_disorders", HLT="", PT="")

    # Check for insomnia
    insomnia_diagnosis_event_found = False
    if PT_text in PT_text_insomnia:
        insomnia_diagnosis_event_found = True
        diagnosis_long_name = PT_text
    if concept_text in concept_text_insomnia:
        insomnia_diagnosis_event_found = True
        diagnosis_long_name = concept_text

    # If we find an insomnia diagnosis event, create it
    if insomnia_diagnosis_event_found:
        event_cnt['id'] += 1
        event_cnt['diagnosis'] += 1
        event_cnt['diagnosis_insomnia'] += 1
        insomnia_diagnosis_event = Event(
            chartdate=date, provenance=date, event_id=event_cnt['id'],
            patient_id=patient_id)
        insomnia_diagnosis_event.diagnosis_role(
            diagnosis_icd9='', diagnosis_name='Insomnia',
            diagnosis_long_name=diagnosis_long_name)
        insomnia_diagnosis_event.roles['concept_text'] = concept_text
        insomnia_diagnosis_event.roles['PT_text'] = PT_text
        events[str(event_cnt['id'])] = insomnia_diagnosis_event


def get_diagnosis_events_distress(events, event_cnt, date, concept_text,
                                  PT_text, patient_id):
    concept_text_distress = ['Emotional distress', 'emotional distress']

    PT_text_distress = ['Emotional distress']

    # Check for distress
    distress_diagnosis_event_found = False
    if PT_text in PT_text_distress:
        distress_diagnosis_event_found = True
        diagnosis_long_name = PT_text
    if concept_text in concept_text_distress:
        distress_diagnosis_event_found = True
        diagnosis_long_name = concept_text

    # If we find a distress diagnosis event, create it
    if distress_diagnosis_event_found:
        event_cnt['id'] += 1
        event_cnt['diagnosis'] += 1
        event_cnt['diagnosis_distress'] += 1
        distress_diagnosis_event = Event(
            chartdate=date, provenance=date, event_id=event_cnt['id'],
            patient_id=patient_id)
        distress_diagnosis_event.diagnosis_role(
            diagnosis_icd9='', diagnosis_name='Distress',
            diagnosis_long_name=diagnosis_long_name)
        distress_diagnosis_event.roles['concept_text'] = concept_text
        distress_diagnosis_event.roles['PT_text'] = PT_text
        events[str(event_cnt['id'])] = distress_diagnosis_event


def format_date(date_obj):
    try:
        date = date_obj.strftime('%Y-%m-%d')
    except AttributeError:
        date = date_obj
    return date


def get_diagnosis_events(events, event_cnt, df):
    columns = dict()
    for column in df.columns.tolist():
        columns[column] = Counter()
    print(df.head())

    for row in df.itertuples():
        #import pdb;pdb.set_trace()
        date = format_date(row.date)
        patient_id = str(row.patid)
        HLGT_text = row.HLGT_text
        PT_text = row.PT_text
        concept_text = row.concept_text

        columns['HLGT_text'][HLGT_text] += 1
        columns['PT_text'][PT_text] += 1
        columns['concept_text'][concept_text] += 1

        # Check for different types of diagnosis events
        get_diagnosis_events_depression(
            events, event_cnt, date, concept_text, PT_text, patient_id)
        get_diagnosis_events_anxiety(
            events, event_cnt, date, concept_text, PT_text, patient_id)
        get_diagnosis_events_insomnia(
            events, event_cnt, date, concept_text, PT_text, patient_id)
        get_diagnosis_events_distress(
            events, event_cnt, date, concept_text, PT_text, patient_id)

    #print(f"columns: {columns}")
    #print("Top 10 diagnosis ")
    #import pdb;pdb.set_trace()


def get_events(df):
    event_cnt = Counter()
    events = {}
    # Diagnosis Events
    get_diagnosis_events(events, event_cnt, df)
    print(f"Found {event_cnt['diagnosis']} diagnosis events")
    print(f"Found {event_cnt['id']} total events")
    return events


def get_patient_visits(df):
    patient_visits = {}
    for row in df.itertuples():
        patient_id = str(row.patid)
        # FIXME, currently using date as visit ID
        visit_id = format_date(row.date)
        if not visit_id:
            continue  # skip if missing ID
        if not patient_visits.get(patient_id, None):
            patient_visits[patient_id] = set()
        patient_visits[patient_id].add(visit_id)
    return patient_visits


def create_visits(
        patient_visits: Dict[str, set]) -> Dict[str, Dict[str, Visit]]:
    """Create visit objects."""
    visits: Dict[str, Dict[str, Visit]] = {}
    for patient_id, visit_set in patient_visits.items():
        if not visits.get(patient_id):
            visits[patient_id] = dict()
        for hadm_id in visit_set:
            date_str = hadm_id
            date_object = datetime.strptime(date_str, "%Y-%m-%d")
            visits[patient_id][hadm_id] = Visit(
                date_object, hadm_id=hadm_id, provenance=patient_id,
                patient_id=patient_id)
    return visits


def attach_events_to_visits(events: Dict[str, Event],
                            visits: Dict[str, Dict[str, Visit]]):
    num_missing_keys = 0
    num_successful_keys = 0

    # Attach events to visits
    for event in events.values():
        try:
            # FIXME, we dont have unique_visit_ids
            visits[event.patient_id][event.provenance].events.append(event)
            num_successful_keys += 1
        except KeyError:
            import pdb
            pdb.set_trace()
            num_missing_keys += 1
    print(f"Events, Num missing keys: {num_missing_keys}\n"
          f"Events, Num successful keys: {num_successful_keys}")


def attach_visits_to_patients(visits, patients, patient_ids):
    num_missing_keys = 0
    num_successful_keys = 0
    patient_ids = [str(x) for x in patient_ids]
    # Attach visits to Patients
    for patient_id in patient_ids:
        patient_visits = visits[patient_id]
        for visit_id, visit in patient_visits.items():
            try:
                patients[str(patient_id)].visits.append(visit)
                num_successful_keys += 1
            except KeyError:
                import pdb
                pdb.set_trace()
                num_missing_keys += 1
    print(f"Vists, Num missing keys: {num_missing_keys}\n"
          f"Visits, Num successful keys: {num_successful_keys}")


def select_non_empty_patients(patient_ids: Set[str],
                              visits: Dict[str, Dict[str, Visit]]) -> Set[int]:
    """Filter out patients IDs with no events"""
    print(
        f"Before filtering out empty patients: {len(patient_ids)} patient IDs")

    non_empty_patient_ids = set()
    for patient_id, patient_visits in visits.items():
        for visit_id, visit in patient_visits.items():
            if visit.events:
                # TODO: should I get patient_id from visit or
                # from key used to iter
                visit_patient_id = int(visit.patient_id)
                non_empty_patient_ids.add(visit_patient_id)

    if not non_empty_patient_ids.issubset(patient_ids):
        print("Non-empty patient IDs are not a subset of patient IDs")
        import pdb
        pdb.set_trace()

    print(
        f"After filtering out empty patients: {len(non_empty_patient_ids)} "
        f"patient IDs")

    return non_empty_patient_ids


def main(args):
    # Setup variables
    output_dir = 'output'
    distinct_column_values_dir = f"{output_dir}/distinct_column_values"
    covid_data_dir = f"/share/pi/stamang/covid/data"
    notes_2019_2020_dir = f"{covid_data_dir}/notes_20190901_20200701/labeled_extractions"
    #notes_2018_2019_dir = f"{covid_data_dir}/notes_20180901_20190701/extracted_notes"

    notes_2019_2020_paths = f"{notes_2019_2020_dir}/all_POS_batch000_099.parquet"
    #notes_2018_2019_paths = f"{notes_2018_2019_dir}/extracted_notes_batch*.parquet"

    # Generate patient dump path
    patients_dump_path = f"{args.output_dir}/patients"
    patients_dump_path_unique = generate_path_with_time(
        path=patients_dump_path, extension='jsonl')

    #path_pattern = f"{notes_2018_2019_dir}/extracted_notes_batch00*.parquet"
    path_pattern = notes_2019_2020_paths
    print(f"path_pattern: {path_pattern}")

    print(f"use_dask: {args.use_dask}")
    meddra_extractions = get_df(path_pattern, args.use_dask)

    columns = sorted(meddra_extractions.columns.tolist())
    print(f"Dataframe column names:\n\t{columns}")

    # Make sure output directories are created
    try:
        os.makedirs(distinct_column_values_dir)
    except OSError:
        pass

    # FIXME: not currently working
    if args.sample_column_values:
        get_distinct_column_values(
            meddra_extractions, distinct_column_values_dir, columns)

    # Get distinct Patient ID values from dataframe
    patient_ids = get_patient_ids(meddra_extractions, args.use_dask)
    print(f"len(patient_ids): {len(patient_ids)}")
    #patient_ids = [str(x) for x in patient_ids]
    #import pdb;pdb.set_trace()

    events = get_events(meddra_extractions)
    if not events:
        print("Empty events dict! Exiting...")
        sys.exit(0)
    print(f"Found {len(events.values())} events values")

    # Get patient visits
    patient_visits = get_patient_visits(meddra_extractions)

    # Create visit objects
    visits = create_visits(patient_visits)
    print(f"len(visits): {len(visits)}")

    # Attach events to visits
    attach_events_to_visits(events, visits)

    # Filter out patient IDs that don't have any visits
    patient_ids = select_non_empty_patients(patient_ids, visits)

    # Generate patients from IDs
    patients = generate_patients_from_ids(patient_ids)

    # Attach visits to patients
    attach_visits_to_patients(visits, patients, patient_ids)

    # Sort patient keys
    sorted_patient_keys = sorted(patients.keys(), key=int)

    #import pdb;pdb.set_trace()

    # Dump patients
    export_patients(patients_dump_path_unique, patients, sorted_patient_keys)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--use_dask', action='store_true')
    parser.add_argument('--sample_column_values', action='store_true')
    parser.add_argument('--output_dir', default="/home/colbyham/output",
                        help='Path to output directory')  # , required=True)
    args: argparse.Namespace = parser.parse_args()
    main(args)
