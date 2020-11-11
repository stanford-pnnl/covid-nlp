import argparse

import pandas as pd

from utils import get_df
from mental_health_analysis import prepare_output_dirs, run_q1, run_q9
from patient_db import PatientDB


def get_command_line_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--patient_db_path',
                        help='Path to load patient_db dump from',
                        required=True)
    parser.add_argument('--output_dir',
                        help='Output dir to dump results',
                        required=True)
    args: argparse.Namespace = parser.parse_args()
    return args


def main(args):
    print("START OF PROGRAM\n")
    # FIXME
    concepts_paths = [
        f'/share/pi/stamang/covid/data/concept/concept00000000000{i}.csv' for i in range(3)]
    print("Loading concepts table")
    frames = [get_df(path) for path in concepts_paths]
    concepts = pd.concat(frames, sort=False)
    # import pdb;pdb.set_trace()

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
        run_q1(patients, question_one_terms,
               f"{args.output_dir}/q1/top_k.jsonl")

    # Q2 - What is the distribution of age groups for patients with major
    #      depression, anxiety, insomnia or distress?
    # question_two_terms = question_one_terms
    # run_q2(patients, question_two_terms, f"{args.output_dir}/q2")

    # Q3 - What is the distribution of age groups and gender groups for
    #      patients with major depression, anxiety, insomnia or distress?
    # question_three_terms = question_one_terms
    # run_q3(patients, question_three_terms, f"{args.output_dir}/q3")

    # Q4 - What is the trend associated with anxiety, loneliness, depression
    #      in both Dx Codes and Clinical Notes?
    # question_four_terms = ['anxiety', 'loneliness', 'depression']
    # run_q4(patients, question_four_terms, f"{args.output_dir}/q4")

    # Q5 - What is the trend assocaited with impaired cognitive function
    #      (Alzheimers, dementia, mild cognitive impairment) in both Dx Codes
    #      and Clinical notes?
    # WHERE ARE DX CODES?
    # question_five_terms = ['alzheimers',
    #                       'dementia', 'mild cognitivie impairment']
    # run_q5(patients, question_five_terms)

    # Q6 - What is the mental health trend associated with older adults with
    #      multi-morbiditty conditions?
    # run_q6(patients)

    # Q7 - What are the top reported causes (anger, anxiety, confusion, fear,
    #      guilt, sadness) for mental health related issues?

    # run_q7()

    # Q8 - What are the distribution of sentiment for mental health related issues?
    # run_q8()

    # Q9 - What are the top medications prescribed for patients with mental health related issues?
    question_nine_top_k, question_nine_cnt_event_type_roles = \
        run_q9(patients, question_one_matches, question_one_event_type_roles,
               concepts, f"{args.output_dir}/q9/top_k.jsonl")

    print("END OF PROGRAM")


if __name__ == "__main__":
    main(get_command_line_args())
