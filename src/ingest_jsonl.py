import json
import os
import sys
from collections import Counter
from pathlib import Path


def dump_patient(patient, path):
    with open(path, 'a+') as fout:
        patient_str = json.dumps(patient)
        fout.write(f"{patient_str}\n")


def main(input_path, output_path, use_first_section, use_first_visit):
    # Remove output file
    try:
        os.remove(output_path)
    except OSError:
        print(f"Can't delete {output_path} as it does not exist")

    c = Counter()
    non_empty_section_headers = Counter()
    counter_num_visits = Counter()
    line_num = -1
    with open(input_path, 'r') as fin:
        for line in fin:
            line_num += 1
            if line_num % 500 == 0:
                print(f"Processing line no: {line_num}", flush=True)
            try:
                patient = json.loads(line)
            except json.JSONDecodeError:
                c['JSONDecodeError'] += 1
                print(f"line_num: {line_num} failed due to JSONDecodeError")
                continue

            patient_id = patient['patient_id']
            c['total_patients'] += 1
            visits = patient['visits']
            num_visits = len(visits)
            counter_num_visits[str(num_visits)] += 1
            # Skip patients with no visits
            if num_visits < 1:
                continue
            non_empty_visits = []
            # Iterate over a patient's visits
            for visit_i, visit in enumerate(visits):
                c['total_visits'] += 1
                # Only use the first visit
                if use_first_visit and visit_i != 0:
                    continue

                # Skip empty section data
                non_empty_section_data = []
                for section_i, section in enumerate(visit['section_data']):
                    c['total_sections'] += 1
                    # Only use the first section
                    if use_first_section and section_i != 0:
                        continue

                    if not section['entity_extraction_results']:
                        c['empty_sections'] += 1
                        continue
                    non_empty_section_headers[section['section_header']] += 1
                    c['non_empty_sections'] += 1
                    non_empty_section_data.append(section)
                # replace section data with non empty section data
                if not non_empty_section_data:
                    c['empty_visits'] += 1
                    continue
                c['non_empty_visits'] += 1
                visit['section_data'] = non_empty_section_data
                non_empty_visits.append(visit)

            if not non_empty_visits:
                c['empty_patients'] += 1
                continue
            c['non_empty_patients'] += 1
            patient['visits'] = non_empty_visits
            patient['patient_id'] = str(patient_id)
            dump_patient(patient, output_path)
            c['dumped_patients'] += 1
    print(c, flush=True)


if __name__ == '__main__':
    use_local = False
    if use_local:
        repo_dir = '/Users/hamc649/Documents/deepcare/covid-19/covid-nlp'
        base_path = 'covid_like_patients_entity_batch000'
    else:
        repo_dir = '/home/colbyham/covid-nlp'
        base_path = 'covid_like_patients_entity'

    n_partitions = 7
    n_patients_per_partition = 1000
    extension = 'jsonl'
    script_name = 'ingest_jsonl'
    input_dir = f'{repo_dir}/{script_name}/input'
    output_dir = f'{repo_dir}/{script_name}/output'

    # Make sure output directory is created
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # only use the first visit
    use_first_visit = False
    # only use the first section for each visit
    use_first_section = True

    # Build output suffix based on bools
    output_suffix = ""
    if use_first_section:
        output_suffix += "-first_section"
    if use_first_visit:
        output_suffix += "-first_visit"

    for partition_i in range(n_partitions):
        print(f"Processing partition {partition_i}")
        input_path = f'{input_dir}/{base_path}.{partition_i}.{extension}'
        output_path = f'{output_dir}/{base_path}{output_suffix}.' \
            f'{partition_i}.{extension}'
        print(f"\tinput_path: {input_path}", flush=True)
        print(f"\toutput_path: {output_path}", flush=True)
        main(input_path, output_path, use_first_section, use_first_visit)
