import argparse
import glob
import json
import os
import sys
import time
from collections import Counter
from datetime import date, datetime
from itertools import product
from typing import Any, Dict, List, Set

import dask
import dask.dataframe as dd
import matplotlib.cbook as cbook
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from tqdm import tqdm

import pyarrow
from data_schema import EntityEncoder, Event, Patient, Visit
from patient_db import PatientDB


def get_df(path, use_dask=False):
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


def get_dates(df, use_dask=False):
    unique_dates = df.date.unique()
    if use_dask:
        unique_dates = unique_dates.compute()

    nunique_dates = df.date.nunique()
    if use_dask:
        nunique_dates = nunique_dates.compute()

    print(f"Found {nunique_dates} dates")
    return unique_dates


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


def get_diagnosis_events_depression(patients: PatientDB,
                                    row,
                                    date_str,
                                    patient_id):
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
        # Create a basic event
        depression_diagnosis_event = Event(
            chartdate=date_str,
            visit_id=date_str,
            patient_id=patient_id)
        # Add diagnosis attributes
        depression_diagnosis_event.diagnosis_role(
            diagnosis_name='Depression',
            diagnosis_long_name=diagnosis_long_name)
        # Add MEDDRA attributes
        depression_diagnosis_event.add_meddra_roles(row)
        patients.add_event(depression_diagnosis_event)

    return depression_diagnosis_event_found


def get_diagnosis_events_anxiety(patients: PatientDB, row, date_str,
                                 patient_id):
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
        anxiety_diagnosis_event = Event(chartdate=date_str, visit_id=date_str,
                                        patient_id=patient_id)
        anxiety_diagnosis_event.diagnosis_role(
            diagnosis_name='Anxiety', diagnosis_long_name=diagnosis_long_name)
        anxiety_diagnosis_event.add_meddra_roles(row)

        patients.add_event(anxiety_diagnosis_event)

    return anxiety_diagnosis_event_found


def get_diagnosis_events_insomnia(patients: PatientDB, row, date_str,
                                  patient_id):
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
        insomnia_diagnosis_event = \
            Event(chartdate=date_str,
                  visit_id=date_str,
                  patient_id=patient_id)
        insomnia_diagnosis_event.diagnosis_role(
            diagnosis_name='Insomnia',
            diagnosis_long_name=diagnosis_long_name)
        insomnia_diagnosis_event.add_meddra_roles(row)
        patients.add_event(insomnia_diagnosis_event)

    return insomnia_diagnosis_event_found


def get_diagnosis_events_distress(patients: PatientDB, row, date_str,
                                  patient_id):
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
        distress_diagnosis_event = \
            Event(chartdate=date_str,
                  visit_id=date_str,
                  patient_id=patient_id)
        distress_diagnosis_event.diagnosis_role(
            diagnosis_name='Distress',
            diagnosis_long_name=diagnosis_long_name)
        distress_diagnosis_event.add_meddra_roles(row)
        patients.add_event(distress_diagnosis_event)
    return distress_diagnosis_event_found


def format_date(date_obj) -> str:
    try:
        date_str = date_obj.strftime('%Y-%m-%d')
    except AttributeError:
        date_str = date_obj
    return date_str

# FIXME


def date_str_to_obj(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    return date_obj


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


def get_diagnosis_events(patients: PatientDB, df):
    print("Getting diagnosis events...")
    columns: Dict[str, Counter] = dict()
    column_names = df.columns.tolist()
    for column in column_names:
        columns[column] = Counter()
    print(df.head())

    # See if you can find a generalizable way to iterate without using
    # itterrows
    # temporarily using to satisfy unkown columns addition to roles

    # FIXME, only look at 1000000 rows
    i_max = 10000000
    print(f"Limiting iteration of dataframe to a maximum of {i_max} rows")
    for i, row in enumerate(df.itertuples()):
        if i % 1000000 == 0:
            now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"{now_str} Tuple: {i}/{i_max}")
        if i >= i_max:
            break
        #import pdb;pdb.set_trace()
        date_str = format_date(row.date)
        patient_id = str(row.patid)

        # Meddra column value counters
        #count_column_values(row, columns)

        # Check for different types of diagnosis events
        found_depression = \
            get_diagnosis_events_depression(
                patients, row, date_str, patient_id)
        found_anxiety = \
            get_diagnosis_events_anxiety(patients, row, date_str, patient_id)
        found_insomnia = \
            get_diagnosis_events_insomnia(patients, row, date_str, patient_id)
        found_distress = \
            get_diagnosis_events_distress(patients, row, date_str, patient_id)

        # If we don't find a mental health symptom assume we have found
        # a diagnosis event/symptom without match
        found_any_events = \
            any([found_depression,
                 found_anxiety,
                 found_insomnia,
                 found_distress])

        if found_any_events:
            continue
        # Add meddra event
        meddra_event = \
            Event(chartdate=date_str, visit_id=date_str, patient_id=patient_id)
        meddra_event.meddra_role(row)
        patients.add_event(meddra_event)

    #print(f"columns: {columns}")
    #print("Top 10 diagnosis ")
    #import pdb;pdb.set_trace()


def get_events(patients: PatientDB, df):
    print("Getting events...")
    # Diagnosis Events
    get_diagnosis_events(patients, df)


def create_patient_visits(patients: PatientDB, patient_visit_dates):
    for i, patient_id, date_str in enumerate(patient_visit_dates):
        if i % 10000 == 0:
            print(f"{now_str()} creating visit {i}/len(patient_visit_dates.keys())")
        visit_id = date_str
        date_obj = date_str_to_obj(date_str)
        visit = Visit(patient_id=str(patient_id), visit_id=visit_id, date=date_obj)
        entity_id = patients.num_visits()
        patients.add_visit(visit, entity_id=entity_id)


def create_patient_visit_dates(patient_ids, date_strs):
    print('Creating patient visit dates')
    print(f"len(patient_ids): {len(patient_ids)}")
    print(f"len(date_str): {len(date_strs)}")
    import pdb;pdb.set_trace()
    #FIXME
    patient_visit_dates = [(patient_id, date_str) for patient_id, date_str in product(patient_ids, date_strs)]
    #for patient_id in patient_ids:
    #    for date_str in date_strs:
    #        visit_id = date_str
    #        date_obj = format_date_str(date_str)
    #        v = (patient_id, visit_id, date_obj)
    #        patient_visit_dates.append(v)

    return patient_visit_dates


def get_patient_visit_dates(patients: PatientDB, df):
    patient_visit_dates = set()
    for row in df.itertuples():
        patient_id = str(row.patid)
        # Skip f missing patient_id
        if not patient_id:
            continue
        date_str = format_date(row.date)
        visit_id = date_str
        visit = (patient_id, visit_id, date_str)
        patient_visit_dates.add(visit)
    return patient_visit_dates


def get_all_patient_visit_dates(patients: PatientDB, df):
    patient_visit_dates = get_patient_visit_dates(patients, df)
    print(f'Found {len(patient_visit_dates)} patient,visit,date tuples')
    return patient_visit_dates


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
    print(f"args: {args}")

    # Create patient DB to store data
    patients = PatientDB(name='all')

    # Get demographics dataframe
    demographics = get_df(args.demographics_path, args.use_dask)

    # Get meddra extractions
    meddra_extractions = get_df(args.meddra_extractions_path, args.use_dask)

    columns = sorted(meddra_extractions.columns.tolist())
    print(f"Dataframe column names:\n\t{columns}")

    patient_ids = get_all_patient_ids(demographics,
                                      meddra_extractions,
                                      args.use_dask)

    get_events(patients, meddra_extractions)
    if not patients.data['events']:
        print("Empty events dict! Exiting...")
        sys.exit(0)
    print(f"Found {patients.num_events()} events")

    print("Filter out patient IDs that don't have any events")
    patient_ids = patients.select_non_empty_patients(patient_ids)

    print('Generate patients from IDs')
    patients.generate_patients_from_ids(patient_ids)
    import pdb
    pdb.set_trace()

    #print('Get all patient visit dates...')
    #patient_visit_dates = \
    # get_all_patient_visit_dates(patients, meddra_extractions)
    #unique_dates = get_dates(meddra_extractions, args.use_dask)
    #unique_date_strs = [format_date(d) for d in unique_dates]
    #patient_visit_dates = \
    #    create_patient_visit_dates(patient_ids, unique_date_strs)

    #print('Creating patient visits...')
    #create_patient_visits(patients, patient_visit_dates)

    #print('Attach visits to patients')
    #patients.attach_visits_to_patients(patient_ids)
    import pdb
    pdb.set_trace()

    # FIXME
    print('Attach events to visits...')
    patients.attach_events_to_visits()

    print('Attach demographic information to patients')
    patients.add_demographic_info(demographics, args.use_dask)
    import pdb
    pdb.set_trace()

    print('Dump patients to a file')
    patients.dump(args.output_dir, "patients", "jsonl", unique=True)

    import pdb
    pdb.set_trace()
    print()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--use_dask', action='store_true')
    parser.add_argument('--sample_column_values', action='store_true')
    parser.add_argument('--demographics_path',
                        default='/share/pi/stamang/covid/data/demo/'
                                'demo_all_pts.parquet')
    parser.add_argument('--meddra_extractions_path',
                        default='/share/pi/stamang/covid/data/'
                                'notes_20190901_20200701/labeled_extractions/'
                                'all_POS_batch000_099.parquet')
    parser.add_argument('--output_dir',
                        default='/home/colbyham/output',
                        help='Path to output directory')  # , required=True)
    args: argparse.Namespace = parser.parse_args()
    main(args)
