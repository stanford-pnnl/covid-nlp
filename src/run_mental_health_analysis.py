import argparse

import pandas as pd
from omop import omop_concept

from utils import get_df, get_table
from mental_health_analysis import prepare_output_dirs, run_q1, run_q9
from patient_db import PatientDB


def get_command_line_args():
    parser = argparse.ArgumentParser()

    # Bools
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--use_dask", action="store_true")

    # Paths
    parser.add_argument(
        "--patient_db_path", help="Path to load patient_db dump from", required=True
    )

    # Dirs
    parser.add_argument(
        "--concept_dir",
        default="/share/pi/stamang/covid/data/concept",
        help="Input dir to read in OMOP CONCEPT table",
    )
    parser.add_argument(
        "--output_dir",
        default="/home/colbyham/output/mental_health_queries",
        help="Output dir to dump results",
    )
    args: argparse.Namespace = parser.parse_args()
    return args


def main(args):
    print("START OF PROGRAM\n")
    # FIXME

    concept_pattern = "*"
    concept_pattern_re = ".*"
    concept = omop_concept(
        args.concept_dir,
        prefix="concept",
        pattern=concept_pattern,
        pattern_re=concept_pattern_re,
        extension=".csv",
        use_dask=args.use_dask,
        debug=args.debug,
    )
    # import pdb;pdb.set_trace()

    # Create and load an instance of PatientDB
    patients = PatientDB(name="all")
    patients.load(args.patient_db_path)

    # Make sure output dirs are created
    prepare_output_dirs(args.output_dir, num_questions=9, prefix="q")

    # Q1 - What are the co-morbidities associated with mental health?
    question_one_terms = ["depression", "anxiety", "insomnia", "distress"]
    (
        question_one_matches,
        question_one_event_type_roles,
        question_one_cnt_event_type_roles,
    ) = run_q1(patients, question_one_terms, f"{args.output_dir}/q1/top_k.jsonl")

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
    question_nine_top_k, question_nine_cnt_event_type_roles = run_q9(
        patients,
        question_one_matches,
        question_one_event_type_roles,
        concept,
        f"{args.output_dir}/q9/top_k.jsonl",
    )

    print("END OF PROGRAM")


if __name__ == "__main__":
    main(get_command_line_args())
