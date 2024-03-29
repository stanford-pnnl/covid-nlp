import json
import os
import re
import sys
from collections import Counter
from copy import deepcopy
from io import StringIO
from pathlib import Path


def format_patient(patient_lines, first_line, last_line):
    patient_lines[-1] = patient_lines[-1].replace("},", "}")
    patient_str = "".join(patient_lines)
    patient_str_full = f"{first_line}{patient_str}{last_line}"
    patient_file = StringIO(patient_str_full)
    patient = json.load(patient_file)
    patient_id = list(patient.keys())[0]
    patient_val = list(patient.values())[0]
    patient_val["patient_id"] = patient_id
    formatted_patient = patient_val

    # print()
    return formatted_patient


def dump_patient(patient, path, c):
    with open(path, "a+") as fout:
        patient_line = json.dumps(patient)
        fout.write(f"{patient_line}\n")

    # Keep track of patients dumped for batching
    c["total_patients_dumped"] += 1
    c["batch_patients_dumped"] += 1
    if c["batch_patients_dumped"] >= c["num_patients_per_batch"]:
        c["batch_patients_dumped"] = 0
        c["batch_id"] += 1
        print(f"Batch: {c['batch_id']}")


def chunk_big_json(
    input_path,
    output_path,
    num_patients_per_batch,
):

    # Pattern to match patient ID
    patient_id_pattern = re.compile(r'^ {4}"\d{8}": {\n$')

    # header and footer defaults
    first_line = "{\n"
    last_line = "}"
    first_loop = True

    # Set up vars
    c = Counter()
    c["line_num"] = -1
    c["batch_id"] = 0
    c["batch_patients_dumped"] = 0
    c["num_patients_per_batch"] = num_patients_per_batch
    patient_lines = []

    ## Remove files in output dir
    # for root, dirs, files in os.walk(output_dir):
    #    for f in files:
    #        print(f"Removing {f}")
    #        os.remove(os.path.join(root, f))

    # Create directory after deleting
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    # sys.exit()

    with open(input_path) as fin:
        for line in fin:
            c["line_num"] += 1
            if c["line_num"] % 100000000 == 0:
                print(f"Processing line no: {c['line_num']}", flush=True)
            # Skip the first line of the entire patients dump
            if first_loop:
                first_loop = False
                continue

            patient_id_match = patient_id_pattern.match(line)
            # Found patient line
            if patient_id_match:
                if patient_lines:
                    patient = format_patient(patient_lines, first_line, last_line)
                    dump_patient(patient, output_path, c)

                    patient_lines.clear()
            patient_lines.append(line)
        # Take care of the last patient
        if patient_lines:
            # Remove the last line for the entire patients dump
            patient_lines.pop()
            patient = format_patient(patient_lines, first_line, last_line)
            dump_patient(patient, output_path, c)

    print(f"{c}")


if __name__ == "__main__":
    repo_dir = "/Users/hamc649/Documents/deepcare/covid-19/covid-nlp"
    input_extension = "json"
    output_extension = "jsonl"
    n_patients_per_partition = 1000
    script_name = "convert_json_to_jsonl"
    script_dir = f"{repo_dir}/{script_name}"
    output_dir = f"{script_dir}/output"

    #input_dir = f"{script_dir}/input/covid_like_patients_24hr"
    input_dir = f"{script_dir}/input"
    
    #base_path = 'entity_risk_by_patients_processed_covidlike_admission_notes'
    #base_path = 'entity_risk_by_patients_processed_covid_admission_notes'
    base_path = 'entity_risk_by_patients_processed_note'

    #input_paths = [f"{base_path}_batch{x}.json" for x in range(16)]
    input_paths = [f'{base_path}.json']

    output_path = f'{output_dir}/{base_path}.jsonl'

    print(f"input_paths: {input_paths}")
    print(f"output_path: {output_path}")
    #sys.exit(0)

    for input_path in input_paths:
        print(f"Processing input_path: {input_path}")
        full_input_path = f"{input_dir}/{input_path}"
        chunk_big_json(
            full_input_path,
            output_path,
            n_patients_per_partition,
        )
