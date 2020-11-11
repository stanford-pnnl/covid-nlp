import argparse
import json
from collections import Counter
from datetime import date
from pathlib import Path

import pandas as pd
from generate import get_concept_name
from utils import get_df
from patient_db import (PatientDB, convert_top_k_concept_ids_to_concept_names,
                        dump_dict, get_top_k, get_unique_match_ids,
                        get_unique_match_patient_visits, print_top_k)


def prepare_output_dirs(output_dir, num_questions=0, prefix=''):
    print('Preparing output directories...')
    for num_q in range(1, num_questions + 1):
        output_dir = f"{output_dir}/{prefix}{num_q}"
        print(f'\tAttempting to create: {output_dir}')
        Path(output_dir).mkdir(parents=True, exist_ok=True)


def run_q1(patients, search_terms, path):
    print("Running Q1...")
    # Find all patients that match at least one of the search terms the roles
    # diagnosis_name or concept_text for DiagnosisEvents and MEDDRAEvents

    # Set up roles to match by event type data structure
    event_type_roles = dict()
    # DiagnosisEvent keys to match on
    event_type_roles['DiagnosisEvent'] = set()
    event_type_roles['DiagnosisEvent'].add('diagnosis_name')
    event_type_roles['DiagnosisEvent'].add('concept_text')
    # MEDDRAEvent keys to match on
    event_type_roles['MEDDRAEvent'] = set()
    event_type_roles['MEDDRAEvent'].add('concept_text')

    matches = \
        patients.match_terms(search_terms, event_type_roles)

    # For all matched visits IDs iterate through and aggregate other event
    # role counts
    unique_patient_visits = get_unique_match_patient_visits(matches)

    cnt_event_type_roles = dict()

    # Set up roles to count on by event type data structure
    cnt_event_type_roles['DiagnosisEvent'] = set()
    # DiagnosisEvent keys to count on
    cnt_event_type_roles['DiagnosisEvent'] = set()
    cnt_event_type_roles['DiagnosisEvent'].add('diagnosis_name')
    cnt_event_type_roles['DiagnosisEvent'].add('concept_text')
    # MEDDRAEvent keys to match on
    cnt_event_type_roles['MEDDRAEvent'] = set()
    cnt_event_type_roles['MEDDRAEvent'].add('concept_text')

    entity_levels = ['patient', 'visit', 'event']
    counters = patients.get_event_counters_from_matches(
        matches, event_type_roles, cnt_event_type_roles,
        entity_levels=entity_levels, patient_visits=unique_patient_visits)

    # Aggregate counts for each such diagnosis code either based on
    # the number of visits or number of patients
    print("\nReporting top-k diagnosis roles...")
    k, top_k = get_top_k(counters, entity_levels, cnt_event_type_roles, k=10)
    print_top_k(top_k, cnt_event_type_roles,
                description=f'Top {k} diagnosis roles per')
    dump_dict(path, top_k)

    return matches, event_type_roles, cnt_event_type_roles


def run_q2():
    print('Running Q2...')


def run_q3():
    print('Running Q3...')


def run_q4():
    print('Running Q4...')


def run_q5():
    print('Running Q5...')


def run_q6():
    print('Running Q6...')


def run_q7():
    print('Running Q7...')


def run_q8():
    print('Running Q8...')


def run_q9(patients, matches, event_type_roles, concepts, path):
    print('Running Q9...')
    cnt_event_type_roles = dict()

    # Set up roles to count on by event type data structure
    cnt_event_type_roles['DRUG_EXPOSURE'] = set()
    #cnt_event_type_roles['DRUG_EXPOSURE'].add('drug_concept_id')
    #cnt_event_type_roles['DRUG_EXPOSURE'].add('drug_type_concept_id')
    cnt_event_type_roles['DRUG_EXPOSURE'].add('drug_concept_name')

    entity_levels = ['patient', 'visit', 'event']
    #entity_levels = ['patient']
    counters = patients.get_event_counters_from_matches(
        matches, event_type_roles, cnt_event_type_roles,
        entity_levels=entity_levels)

    # Aggregate counts for each such diagnosis code either based on
    # the number of visits or number of patients
    print("\nReporting top-k DRUG_EXPOSURE roles...")
    k, top_k = get_top_k(counters, entity_levels, cnt_event_type_roles, k=20)
    #top_k = convert_top_k_concept_ids_to_concept_names(
    #    top_k, cnt_event_type_roles, concepts)
    print_top_k(top_k, cnt_event_type_roles,
                description=f'Top {k} DRUG_EXPOSURE roles per')
    dump_dict(path, top_k)
    import pdb;pdb.set_trace()
    return top_k, cnt_event_type_roles


# FIXME broken
def test_split_by_month(patients):
    print('Test split patient DB by monthly frequency key')
    monthly_splits = patients.agg_time(time_freq='M')
    for month_db in monthly_splits.values():
        print(f"{month_db}")


# FIXME broken
def mental_health_age_distribution(patients, search_terms, output_dir):
    all_patients_dist_path = f"{output_dir}/all_patients_dist.png"
    matched_patients_dist_path = \
        f"{output_dir}/matched_patients_dist.png"

    # Match patients based on search terms
    matched_patients, matches = patients.match_terms(search_terms, [
        'diagnosis_name', 'concept_text'], ['DiagnosisEvent', 'MEDDRAEvent'])

    # Set compare date for age calculation to today
    compare_date = date.today()

    # Calulate age/gender distribution from input patients DB
    min_age, max_age = patients.calculate_patient_ages(compare_date)
    patients.calculate_age_gender_distribution(
        min_age, max_age,
        patients.get_unique_genders(),
        all_patients_dist_path)
    print(f"input patients: min_age: {min_age}, max_age: {max_age}")

    # Calculate age/gender distribution from matched patients DB
    min_age, max_age = matched_patients.calculate_patient_ages(compare_date)
    matched_patients.calculate_age_gender_distribution(
        min_age, max_age,
        matched_patients.get_unique_genders(),
        matched_patients_dist_path)
    print(f"matched patients: min_age: {min_age}, max_age: {max_age}")
    #import pdb
    #pdb.set_trace()
    print()

    # Calculate age/gener distribution for each monthly split
    monthly_splits = patients.agg_time('M')
    for key, month_split in monthly_splits.items():
        month_split_dist_path = \
            f"{output_dir}/{key}_dist.png"
        print(month_split_dist_path)
        min_age, max_age = month_split.calculate_patient_ages(compare_date)
        month_split.calculate_age_gender_distribution(
            min_age, max_age,
            month_split.get_unique_genders(),
            month_split_dist_path
        )
