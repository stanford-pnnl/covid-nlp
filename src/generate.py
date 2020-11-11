import argparse
import os
import sys
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
import pyarrow
from dask.core import get
from tqdm import tqdm

from data_schema import EntityEncoder, Event, Patient, Visit
from events import get_events
from omop import omop_concept, omop_drug_exposure
from patient_db import PatientDB
from utils import (date_obj_to_str, date_str_to_obj, datetime_obj_to_str,
                   datetime_str_to_obj, get_df, get_df_frames, get_patient_ids,
                   get_person_ids, get_table)


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
    counter['date'][date_obj_to_str(row.date)] += 1
    counter['extracted_CUI'][row.extracted_CUI] += 1
    counter['medID'][row.medID] += 1
    counter['note_id'][row.note_id] += 1
    counter['note_title'][row.note_title] += 1
    counter['patid'][row.patid] += 1
    counter['polarity'][row.polarity] += 1
    counter['pos'][row.pos] += 1
    counter['present'][row.present] += 1
    counter['ttype'][row.ttype] += 1


def get_concept(df, concept_id):
    concept = df[df.concept_id == concept_id].iloc[0]
    return concept


def get_concept_name(df, concept_id) -> str:
    concept = get_concept(df, concept_id)
    concept_name = concept.concept_name
    return concept_name


def get_concept_class_id(df, concept_id):
    concept = get_concept(df, concept_id)
    concept_class_id = concept.concept_class_id
    return concept_class_id, concept


def print_concept(df, concept_id):
    concept = get_concept(df, concept_id)
    print(concept)
    return concept


def create_patient_visit_dates(patient_ids, date_strs):
    print('Creating patient visit dates')
    print(f"len(patient_ids): {len(patient_ids)}")
    print(f"len(date_str): {len(date_strs)}")
    #import pdb
    #pdb.set_trace()
    # FIXME
    patient_visit_dates = \
        [(patient_id, date_str)
         for patient_id, date_str in product(patient_ids, date_strs)]
    # for patient_id in patient_ids:
    #    for date_str in date_strs:
    #        visit_id = date_str
    #        date_obj = date_str_to_obj(date_str)
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
        date_str = date_obj_to_str(row.date)
        visit_id = date_str
        visit = (patient_id, visit_id, date_str)
        patient_visit_dates.add(visit)
    return patient_visit_dates


def get_all_patient_visit_dates(patients: PatientDB, df):
    patient_visit_dates = get_patient_visit_dates(patients, df)
    print(f'Found {len(patient_visit_dates)} patient,visit,date tuples')
    return patient_visit_dates


def get_all_patient_ids(demographics, extractions, drug_exposure,
                        use_dask=False):
    all_patient_ids = set()

    demo_person_ids = get_person_ids(demographics, use_dask)
    demo_person_ids = set(demo_person_ids)
    print(f"demographics: len(demo_person_ids): {len(demo_person_ids)}")
    all_patient_ids.update(demo_person_ids)

    # Get distinct Patient ID values from dataframe
    patient_ids = get_patient_ids(extractions, use_dask)
    patient_ids = set(patient_ids)
    print(f"extractions: len(patient_ids): {len(patient_ids)}")
    all_patient_ids.update(patient_ids)

    drug_exposure_person_ids = get_person_ids(drug_exposure, use_dask)
    #import pdb;pdb.set_trace()
    # DEBUG FIXME, dask doesn't like turning this to a set
    drug_exposure_person_ids = set(drug_exposure_person_ids)
    print(f"medications: len(med_person_ids): {len(drug_exposure_person_ids)}")
    all_patient_ids.update(drug_exposure_person_ids)

    print(f"len(all_patient_ids): {len(all_patient_ids)}")
    return all_patient_ids


def generate_patient_db(demographics_path, meddra_extractions_dir,
                        drug_exposure_dir, concept_dir, output_dir, debug,
                        use_dask):

    # Create patient DB to store data
    patients = PatientDB(name='all')

    # Get demographics dataframe
    demographics = get_df(demographics_path,
                          use_dask=use_dask,
                          debug=debug)

    ### NLP TABLES ###
    # Get meddra extractions dataframe
    meddra_extractions_pattern = '*_*'
    meddra_extractions = get_table(meddra_extractions_dir,
                                   prefix='all_POS_batch',
                                   pattern=meddra_extractions_pattern,
                                   extension='.parquet',
                                   use_dask=use_dask,
                                   debug=debug)

    ### OMOP TABLES ###
    # OMOP DRUG_EXPOSURE table
    drug_exposure = omop_drug_exposure(drug_exposure_dir,
                                       use_dask=use_dask,
                                       debug=debug)

    # OMOP CONCEPT table
    concept = omop_concept(concept_dir,
                           use_dask=use_dask,
                           debug=debug)
    
    import pdb;pdb.set_trace()

    columns = sorted(meddra_extractions.columns.tolist())
    print(f"Dataframe column names:\n\t{columns}")

    patient_ids = get_all_patient_ids(demographics,
                                      meddra_extractions,
                                      drug_exposure,
                                      use_dask=use_dask)

    get_events(patients, concept, meddra_extractions, drug_exposure,
               use_dask=False)
    if not patients.data['events']:
        print("Empty events dict! Exiting...")
        sys.exit(0)
    print(f"Found {patients.num_events()} events")

    print("Filter out patient IDs that don't have any events")
    patient_ids = patients.select_non_empty_patients(patient_ids)

    print('Generate patients from IDs')
    patients.generate_patients_from_ids(patient_ids)
    #import pdb
    #pdb.set_trace()

    #print('Get all patient visit dates...')
    # patient_visit_dates = \
    # get_all_patient_visit_dates(patients, meddra_extractions)
    #unique_dates = get_dates(meddra_extractions, args.use_dask)
    #unique_date_strs = [date_obj_to_str(d) for d in unique_dates]
    # patient_visit_dates = \
    #    create_patient_visit_dates(patient_ids, unique_date_strs)

    #print('Creating patient visits...')
    #create_patient_visits(patients, patient_visit_dates)

    #print('Attach visits to patients')
    # patients.attach_visits_to_patients(patient_ids)
    #import pdb
    # pdb.set_trace()

    # FIXME
    print('Attach events to visits...')
    patients.attach_events_to_visits()
    #import pdb
    #pdb.set_trace()

    print('Attach demographic information to patients')
    patients.add_demographic_info(demographics, use_dask)
    #import pdb
    #pdb.set_trace()

    print('Dump patients to a file')
    patients.dump(output_dir, "patients", "jsonl", unique=True)

    #import pdb
    #pdb.set_trace()
    print()
