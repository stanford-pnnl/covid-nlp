import argparse
import json
from collections import Counter
from datetime import date
from patient_db import PatientDB, get_top_k


def match_terms(patients, terms):
    print(f"Matching terms:\n\t{terms}\n")
    print(f"Matching against:\n\t{patients}\n")
    patients_matched = PatientDB(name='patients_matched')
    matches = set()
    for term in terms:
        print(f"{term}")
        term_patients_matched, term_matches = \
            patients.match_patients(
                f'{term}_patients_matched',
                term,
                event_keys=['diagnosis_name'],
                event_types=['DiagnosisEvent'])
        patients_matched.merge_patients(term_patients_matched)
        matches = matches.union(term_matches)
        print(f"\tresult: {term_patients_matched}\n")
    print(f"All patients matched:\n\tresult: {patients_matched}")
    return patients_matched, matches


def mental_health_co_morbidities(patients, search_terms):
    print("Running mental health co-morbidities checks...")
    patients_matched, matches = match_terms(patients, search_terms)

    event_counters, event_roles, event_entity_levels = \
        patients_matched.get_event_counters(
            event_types=['DiagnosisEvent'], meddra_roles=True)
    #import pdb;pdb.set_trace()

    # Aggregate counts for each such diagnosis code either based on
    # the number of visits or number of patients
    print("\nReporting top-k diagnosis roles...")
    k, top_k = get_top_k(event_counters,
                         event_entity_levels,
                         event_roles,
                         k=10)
    for entity_level in top_k:
        print(f"Top {k} diagnosis roles per {entity_level}:")
        for key in sorted(top_k[entity_level]):
            values = top_k[entity_level][key]
            if values:
                values_str = [f"\t\t{v}\n" for v in values]
                values_str = "".join(values_str)
                print(f"\tkey: {key}\n{values_str}")
    #import pdb;pdb.set_trace()


def test_split_by_month(patients):
    print('Test split patient DB by monthly frequency key')
    monthly_splits = patients.agg_time(time_freq='M')
    for month_db in monthly_splits.values():
        print(f"{month_db}")


def mental_health_age_distribution(patients, search_terms, output_dir):
    all_patients_age_dist_path = f"{output_dir}/all_patients_age_dist.png"
    matched_patients_age_dist_path = \
        f"{output_dir}/matched_patients_age_dist.png"

    # Match patients based on search terms
    matched_patients, matches = match_terms(patients, search_terms)

    compare_date = date.today()
    # Calulate age distribution from input patients DB
    min_age, max_age = patients.calculate_patient_ages(compare_date)
    patients.calculate_age_gender_distribution(
        min_age, max_age, patients.get_unique_genders(), all_patients_age_dist_path)
    print(f"input patients: min_age: {min_age}, max_age: {max_age}")

    # Calculate age distribution from matched patients DB
    min_age, max_age = matched_patients.calculate_patient_ages(compare_date)
    matched_patients.calculate_age_gender_distribution(
        min_age, max_age, matched_patients.get_unique_genders(),
        matched_patients_age_dist_path)
    print(f"matched patients: min_age: {min_age}, max_age: {max_age}")
    import pdb
    pdb.set_trace()


def main(args):
    print("START OF PROGRAM\n")
    # Create and load an instance of PatientDB
    patients = PatientDB(name='all')
    patients.load(args.patient_db_path)

    print("\nQuestion 1:")
    question_one_terms = ['Depression', 'Anxiety', 'Insomnia', 'Distress']
    mental_health_co_morbidities(patients, question_one_terms)

    # Q2
    print("\nQuestion 2:")
    question_two_terms = question_one_terms
    mental_health_age_distribution(patients, question_two_terms,
                                   args.output_dir)

    # Q3
    #question_three_terms = question_one_terms
    #mental_health_age_sex_distribution(patients, question_three_terms)
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
