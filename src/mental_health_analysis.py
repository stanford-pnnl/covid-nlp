import argparse
import json
from collections import Counter
from datetime import date
from pathlib import Path

from patient_db import (PatientDB, get_top_k, get_unique_match_ids,
                        get_unique_match_patient_visits)


def prepare_output_dirs(output_dir, num_questions=0, prefix=''):
    print('Preparing output directories...')
    for num_q in range(1, num_questions + 1):
        output_dir = f"{args.output_dir}/q{num_q}"
        print(f'\tAttempting to create: {output_dir}')
        Path(output_dir).mkdir(parents=True, exist_ok=True)


def run_q2(patients, search_terms, output_dir):
    print('Running Q2...')
    #mental_health_age_distribution(patients, search_terms, output_dir)


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

def run_q9(patients, matches, event_type_roles):
    print('Running Q9...')
    cnt_event_type_roles = dict()

    # Set up roles to count on by event type data structure
    cnt_event_type_roles['DRUG_EXPOSURE'] = set()
    cnt_event_type_roles['DRUG_EXPOSURE'].add('drug_concept_id')
    cnt_event_type_roles['DRUG_EXPOSURE'].add('drug_type_concept_id')

    entity_levels = ['patient', 'visit', 'event']
    counters = patients.get_event_counters_from_matches(
        matches, event_type_roles, cnt_event_type_roles,
        entity_levels=entity_levels)
    
    import pdb;pdb.set_trace()
    print()


def run_q1(patients, search_terms):
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
    #unique_match_ids = get_unique_match_ids(matches)
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
    counters = patients.get_counters_from_matches(
        matches, unique_patient_visits, event_type_roles, cnt_event_type_roles,
        entity_levels=entity_levels)


    # Aggregate counts for each such diagnosis code either based on
    # the number of visits or number of patients
    print("\nReporting top-k diagnosis roles...")
    k, top_k = get_top_k(counters, entity_levels, cnt_event_type_roles, k=10)

    for entity_level in top_k:
        print(f"Top {k} diagnosis roles per {entity_level}:")
        for event_type, event_roles in cnt_event_type_roles.items():
            print(f"\tEvent type: {event_type}")
            for event_role in sorted(event_roles):
                values = top_k[entity_level][event_type][event_role]
                if values:
                    values_str = [f"\t\t\t{v}\n" for v in values]
                    values_str = "".join(values_str)
                    print(f"\t\tevent_role: {event_role}\n{values_str}")
    import pdb;pdb.set_trace()
    print()
    return matches, event_type_roles, cnt_event_type_roles


def test_split_by_month(patients):
    print('Test split patient DB by monthly frequency key')
    monthly_splits = patients.agg_time(time_freq='M')
    for month_db in monthly_splits.values():
        print(f"{month_db}")


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
    import pdb
    pdb.set_trace()
    print()

    # Calculate age/gener distribution for each monthly split
    monthly_splits = patients.agg_time('M')
    for key, month_split in monthly_splits.items():
        month_split_dist_path = \
            f"{args.output_dir}/{key}_dist.png"
        print(month_split_dist_path)
        min_age, max_age = month_split.calculate_patient_ages(compare_date)
        month_split.calculate_age_gender_distribution(
            min_age, max_age,
            month_split.get_unique_genders(),
            month_split_dist_path
        )


def main(args):
    print("START OF PROGRAM\n")
    # Create and load an instance of PatientDB
    patients = PatientDB(name='all')
    patients.load(args.patient_db_path)

    # Make sure output dirs are created
    prepare_output_dirs(args.output_dir, num_questions=9, prefix='q')

    # Q1 - What are the co-morbidities associated with mental health?
    question_one_terms = ['depression', 'anxiety', 'insomnia', 'distress']
    question_one_matches,\
        question_one_event_type_roles,\
            question_one_cnt_event_type_roles = \
        run_q1(patients, question_one_terms)

    # Q2 - What is the distribution of age groups for patients with major
    #      depression, anxiety, insomnia or distress?
    #question_two_terms = question_one_terms
    #run_q2(patients, question_two_terms, f"{args.output_dir}/q2")

    # Q3 - What is the distribution of age groups and gender groups for
    #      patients with major depression, anxiety, insomnia or distress?
    #question_three_terms = question_one_terms
    #run_q3(patients, question_three_terms, f"{args.output_dir}/q3")

    # Q4 - What is the trend associated with anxiety, loneliness, depression
    #      in both Dx Codes and Clinical Notes?
    #question_four_terms = ['anxiety', 'loneliness', 'depression']
    #run_q4(patients, question_four_terms, f"{args.output_dir}/q4")

    # Q5 - What is the trend assocaited with impaired cognitive function
    #      (Alzheimers, dementia, mild cognitive impairment) in both Dx Codes
    #      and Clinical notes?
    # WHERE ARE DX CODES?
    #question_five_terms = ['alzheimers',
    #                       'dementia', 'mild cognitivie impairment']
    #run_q5(patients, question_five_terms)

    # Q6 - What is the mental health trend associated with older adults with
    #      multi-morbiditty conditions?
    #run_q6(patients)

    # Q7 - What are the top reported causes (anger, anxiety, confusion, fear,
    #      guilt, sadness) for mental health related issues?

    #run_q7()
    
    # Q8 - What are the distribution of sentiment for mental health related issues?
    #run_q8()

    # Q9 - What are the top medications prescribed for patients with mental health related issues?
    run_q9(patients, question_one_matches, question_one_event_type_roles)

    print("END OF PROGRAM")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--patient_db_path',
                        help='Path to load patient_db dump from',
                        required=True)
    parser.add_argument('--output_dir',
                        help='Output dir to dump results',
                        required=True)
    args: argparse.Namespace = parser.parse_args()
    main(args)
