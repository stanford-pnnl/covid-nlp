import argparse
import glob
import json
import os
import sys
import time
from collections import Counter
from datetime import date, datetime
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
from patient_db import PatientDB

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


def get_person_ids(df, use_dask=False):
    unique_person_ids = df.person_id.unique()
    if use_dask:
        unique_person_ids = unique_person_ids.compute()

    nunique_person_ids = df.person_id.nunique()
    if use_dask:
        nunique_person_ids = nunique_person_ids.compute()

    print(f"Found {nunique_person_ids} person IDs")
    return unique_person_ids


def get_patient_ids(df, use_dask=False):
    unique_patient_ids = df.patid.unique()
    if use_dask:
        unique_patient_ids = unique_patient_ids.compute()

    nunique_patient_ids = df.patid.nunique()
    if use_dask:
        nunique_patient_ids = nunique_patient_ids.compute()

    print(f"Found {nunique_patient_ids} patient IDs")
    return unique_patient_ids



def generate_path_with_time(path: str, extension: str) -> str:
    """Generate path string with time included."""
    timestr = time.strftime("%Y%m%d-%H%M%S")
    time_path = f"{path}_{timestr}.{extension}"

    return time_path


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


def get_diagnosis_events_depression(events, event_cnt, row, date, patient_id):
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
    PT_text = row.PT_text
    concept_text = row.concept_text

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
        add_meddra_roles(depression_diagnosis_event, row)
        events[str(event_cnt['id'])] = depression_diagnosis_event

    return depression_diagnosis_event_found

def get_diagnosis_events_anxiety(events, event_cnt, row, date, patient_id):
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

    PT_text = row.PT_text
    concept_text = row.concept_text

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
        add_meddra_roles(anxiety_diagnosis_event, row)
        events[str(event_cnt['id'])] = anxiety_diagnosis_event
    return anxiety_diagnosis_event_found
# TODO match with ability to define meddra level matches


def get_diagnosis_events_insomnia(events, event_cnt, row, date, patient_id):
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
    PT_text = row.PT_text
    concept_text = row.concept_text

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
        add_meddra_roles(insomnia_diagnosis_event, row)
        events[str(event_cnt['id'])] = insomnia_diagnosis_event
    return insomnia_diagnosis_event_found

def get_diagnosis_events_distress(events, event_cnt, row, date, patient_id):
    concept_text_distress = ['Emotional distress', 'emotional distress']

    PT_text_distress = ['Emotional distress']
    PT_text = row.PT_text
    concept_text = row.concept_text

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
        add_meddra_roles(distress_diagnosis_event, row)
        events[str(event_cnt['id'])] = distress_diagnosis_event
    return distress_diagnosis_event_found

def format_date(date_obj):
    try:
        date = date_obj.strftime('%Y-%m-%d')
    except AttributeError:
        date = date_obj
    return date


def add_meddra_roles(event, row):
    # Meddra levels
    event.roles['SOC'] = row.SOC
    event.roles['HLGT'] = row.HLGT
    event.roles['HLT'] = row.HLT
    event.roles['PT'] = row.PT

    # Meddra CUI levels
    event.roles['SOC_CUI'] = row.SOC_CUI
    event.roles['HLGT_CUI'] = row.HLGT_CUI
    event.roles['HLT_CUI'] = row.HLT_CUI
    event.roles['PT_CUI'] = row.PT_CUI
    event.roles['extracted_CUI'] = row.extracted_CUI

    # Meddra text levels
    event.roles['SOC_text'] = row.SOC_text
    event.roles['HLGT_text'] = row.HLGT_text
    event.roles['HLT_text'] = row.HLT_text
    event.roles['PT_text'] = row.PT_text
    event.roles['concept_text'] = row.concept_text

    # Everything else (not adding date twice)
    event.roles['PExperiencer'] = row.PExperiencer
    event.roles['medID'] = row.medID
    event.roles['note_id'] = row.note_id
    event.roles['note_title'] = row.note_title
    event.roles['polarity'] = row.polarity
    event.roles['pos'] = row.pos
    event.roles['present'] = row.present
    event.roles['ttype'] = row.ttype

def count_column_values(row, counter):
    counter['HLGT'][row.HLGT] += 1
    counter['HLGT_CUI'][row.HLGT_CUI] += 1
    counter['HLGT_text'][row.HLGT_text] += 1
    counter['HLT'][row.HLT] += 1
    counter['HLT_CUI'][row.HLT_CUI] += 1
    counter['HLT_text'][row.HLT_text] += 1
    counter['PExperiencer'][row.PExperiencer] += 1
    counter['PT'][row.PT] += 1
    counter['PT_CUI'][row.PT_CUI] += 1
    counter['PT_text'][row.PT_text] += 1
    counter['SOC'][row.SOC] += 1
    counter['SOC_CUI'][row.SOC_CUI] += 1
    counter['SOC_text'][row.SOC_text] += 1
    counter['concept_text'][row.concept_text] += 1
    counter['date'][format_date(row.date)] += 1
    counter['extracted_CUI'][row.extracted_CUI] += 1
    counter['medID'][row.medID] += 1
    counter['note_id'][row.note_id] += 1
    counter['note_title'][row.note_title] += 1
    counter['patid'][row.patid] += 1
    counter['polarity'][row.polarity] += 1
    counter['pos'][row.pos] += 1
    counter['present'][row.present] += 1
    counter['ttype'][row.ttype] += 1

def get_diagnosis_events(events, event_cnt, df):
    columns = dict()
    column_names = df.columns.tolist()
    for column in column_names:
        columns[column] = Counter()
    print(df.head())

    # See if you can find a generalizable way to iterate without using itterrows
    # temporarily using to satisfy unkown columns addition to roles
    
    #FIXME, only look at 1000000 rows
    row_seen_max = 1000000
    row_seen = 0
    for row in df.itertuples():
        row_seen += 1
        if row_seen % 10000 == 0:
            print(f"Row: {row_seen}")
        if row_seen > row_seen_max:
            break
        #import pdb;pdb.set_trace()
        date = format_date(row.date)
        patient_id = str(row.patid)
        
        # Meddra column value counters
        #count_column_values(row, columns)

        # Check for different types of diagnosis events
        found_depression = get_diagnosis_events_depression(events, event_cnt, row, date, patient_id)
        found_anxiety = get_diagnosis_events_anxiety(events, event_cnt, row, date, patient_id)
        found_insomnia = get_diagnosis_events_insomnia(events, event_cnt, row, date, patient_id)
        found_distress = get_diagnosis_events_distress(events, event_cnt, row, date, patient_id)

        # If we don't find a mental health symptom assume we have found
        # a diagnosis event/symptom without match
        found_any_events = found_depression or found_anxiety or found_insomnia or found_distress
        #FIXME
        #found_any_events = False
        if found_any_events:
            continue
        # Add symptom event
        event_cnt['id'] += 1
        event_cnt['diagnosis'] += 1
        event_cnt['diagnosis_symptom'] += 1
        diagnosis_event = Event(chartdate=date, provenance=date, event_id=event_cnt['id'], patient_id=patient_id)
        diagnosis_name = row.concept_text
        diagnosis_long_name = row.concept_text
        diagnosis_event.diagnosis_role(diagnosis_icd9='', diagnosis_name=diagnosis_name, diagnosis_long_name=diagnosis_long_name)

        # Add meddra items
        add_meddra_roles(diagnosis_event, row)


        events[str(event_cnt['id'])] = diagnosis_event

    #print(f"columns: {columns}")
    #print("Top 10 diagnosis ")
    #import pdb;pdb.set_trace()


def get_events(df):
    print("Getting events...")
    event_cnt = Counter()
    events = {}
    # Diagnosis Events
    get_diagnosis_events(events, event_cnt, df)
    print(f"Found {event_cnt['diagnosis']} diagnosis events")
    print(f"Found {event_cnt['id']} total events")
    print(f"{event_cnt}")
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


def get_all_patient_ids(demographics, extractions, use_dask):
    all_patient_ids = set()

    person_ids = get_person_ids(demographics, use_dask)
    person_ids = set(person_ids)
    print(f"len(person_ids): {len(person_ids)}")
    all_patient_ids.update(person_ids)

    # Get distinct Patient ID values from dataframe
    patient_ids = get_patient_ids(extractions, use_dask)
    patient_ids = set(patient_ids)
    print(f"len(patient_ids): {len(patient_ids)}")
    all_patient_ids.update(patient_ids)
   
    print(f"len(all_patient_ids): {len(all_patient_ids)}")
    return all_patient_ids


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

    # Get demographics dataframe
    demographics_path = f"{covid_data_dir}/demo/demo_all_pts.parquet"
    demographics = get_df(demographics_path, args.use_dask)

    # Get meddra extractions
    meddra_extractions = get_df(path_pattern, args.use_dask)

    columns = sorted(meddra_extractions.columns.tolist())
    print(f"Dataframe column names:\n\t{columns}")

    
    patient_ids = get_all_patient_ids(demographics, meddra_extractions, args.use_dask)

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

    patients = PatientDB(name='non_empty')
    # Generate patients from IDs
    patients.generate_patients_from_ids(patient_ids)
    import pdb;pdb.set_trace()

    # Attach demographic information to patients
    patients.add_demographic_info(demographics, args.use_dask)
    import pdb;pdb.set_trace()

    # Attach visits to patients
    patients.attach_visits_to_patients(visits, patient_ids)
    import pdb;pdb.set_trace()

    try:
        # Dump patients to a file
        patients.dump(patients_dump_path_unique)
    except:
        pass
    import pdb;pdb.set_trace()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--use_dask', action='store_true')
    parser.add_argument('--sample_column_values', action='store_true')
    parser.add_argument('--output_dir', default="/home/colbyham/output",
                        help='Path to output directory')  # , required=True)
    args: argparse.Namespace = parser.parse_args()
    main(args)
