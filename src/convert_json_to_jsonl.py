import json
import os
import re
from copy import deepcopy
from io import StringIO


def format_patient(patient_lines, first_line, last_line):
    patient_lines[-1] = patient_lines[-1].replace('},', '}')
    patient_str = ''.join(patient_lines)
    patient_str_full = f"{first_line}{patient_str}{last_line}"
    patient_file = StringIO(patient_str_full)
    patient = json.load(patient_file)
    patient_id = list(patient.keys())[0]
    patient_val = list(patient.values())[0]
    patient_val['patient_id'] = patient_id
    formatted_patient = patient_val
    
    print()
    return formatted_patient


def dump_patient(patient, path):
    with open(path, 'a+') as fout:
        patient_line = json.dumps(patient)
        fout.write(f"{patient_line}\n")


def chunk_big_json(input_path, output_path, num_patients_per_file):
    # Pattern to match patient ID
    patient_id_pattern = re.compile(r'^ {4}"\d{8}": {\n$')

    # header and footer defaults
    first_line = '{\n'
    last_line = '}'
    first_loop = True

    patient_lines = []

    # Remove output file
    try:
        os.remove(output_path)
    except OSError:
        print(f"Can't delete {output_path} as it does not exist")

    line_num = -1
    with open(input_path) as fin:
        for line in fin:
            line_num += 1
            if line_num % 1000000 == 0:
                print(f"Processing line no: {line_num}", flush=True)  
            # Skip the first line of the entire patients dump
            if first_loop:
                first_loop = False
                continue
            patient_id_match = patient_id_pattern.match(line)
            # Found patient line
            if patient_id_match:
                if patient_lines:
                    patient = \
                        format_patient(patient_lines, first_line, last_line)
                    dump_patient(patient, output_path)
                    patient_lines.clear()
            patient_lines.append(line)
        # Take care of the last patient
        if patient_lines:
            # Remove the last line for the entire patients dump
            patient_lines.pop()
            patient = format_patient(patient_lines, first_line, last_line)
            dump_patient(patient, output_path)
        print()


if __name__ == '__main__':
    use_local = False
    if use_local:
        repo_dir = '/Users/hamc649/Documents/deepcare/covid-19/covid-nlp'
        base_path = 'covid_like_patients_entity_batch000'
    else:
        repo_dir = '/home/colbyham/covid-nlp'
        base_path = 'covid_like_patients_entity'
    input_path = f'{repo_dir}/input/{base_path}.json'
    output_path = f'{repo_dir}/input/{base_path}.jsonl'
    chunk_big_json(input_path, output_path, 1)
