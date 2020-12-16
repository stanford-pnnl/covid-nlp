import json
import sys
from collections import Counter
import os

def dump_patient(patient, path):
    with open(path, 'a+') as fout:
        patient_str = json.dumps(patient)
        fout.write(f"{patient_str}\n")

def main(input_path, output_path):
    # Remove output file
    try:
        os.remove(output_path)
    except OSError:
        print(f"Can't delete {output_path} as it does not exist")

    use_first_visit = True
    c = Counter()
    non_empty_section_headers = Counter()
    counter_num_visits = Counter()
    line_num = -1
    with open(input_path, 'r') as fin:
        for line in fin:
            line_num += 1
            if line_num % 10000 == 0:
                print(f"Processing line no: {line_num}", flush=True)
            patient = json.loads(line)
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
                for section in visit['section_data']:
                    c['total_sections'] += 1
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
    print()


if __name__ == '__main__':
    use_local = False
    if use_local:
        repo_dir = '/Users/hamc649/Documents/deepcare/covid-19/covid-nlp'
        base_path = 'covid_like_patients_entity_batch000'
    else:
        repo_dir = '/home/colbyham/covid-nlp'
        base_path = 'covid_like_patients_entity'

    input_path = f'{repo_dir}/input/{base_path}.jsonl'
    output_path = f'{repo_dir}/output/{base_path}-first_visit.jsonl'
    main(input_path, output_path)
