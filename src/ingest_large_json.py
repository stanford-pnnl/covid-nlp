import json
from collections import Counter
#import ijson
from pathlib import Path


def main():
    use_ijson = False
    use_first_visit = True
    use_covid_like = True

    # Local desktop paths
    #repo_dir = '/Users/hamc649/Documents/deepcare/covid-19/covid-nlp'
    #patients_batch_path = f"{repo_dir}/input/covid_like_patients_entity_batch000.json"

    # Nero GCP paths
    repo_dir = '/home/colbyham/covid-nlp'
    
    if use_covid_like:
        base_path = 'covid_like_patients_entity'
    else:
        base_path = 'covid_patients_entity'
    patients_path = f"{repo_dir}/input/{base_path}.json"

    output_dir = f"{repo_dir}/output/entity_extraction"
    # Make sure output diretory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    if use_first_visit:
        output_path = f"{output_dir}/{base_path}-first_visit_non_empty.jsonl"
    else:
        output_path = f"{output_dir}/{base_path}-all_visit_non_empty.jsonl"
    print(f"output_path: {output_path}")
    counter_num_visits = Counter()
    c = Counter()
    non_empty_section_headers = Counter()

    patients = []
    
    fin = open(patients_path, 'r')
    fout = open(output_path, 'w')

    if use_ijson:
        print("Using ijson")
        #batches = ijson.items(fin, '')
        #batch = batches[0]
    else:
        print("Not using ijson")
        batch = json.load(fin)
    print()
    

    # Iterate over patients
    for patient_id, p in batch.items():
        c['total_patients'] += 1
        print(f"patient_id: {patient_id}", flush=True)
        #if patient_id != '29923647':
        #    continue
        first_visit = None
        #print(k)
        visits = p['visits']
        num_visits = len(visits)
        counter_num_visits[str(num_visits)] += 1
        # Skip patients with no visits
        if num_visits < 1:
            continue
        non_empty_visits = []
        # Iterate over a patients' visits
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
        p['visits'] = non_empty_visits
        
        p['patient_id'] = str(patient_id)
        
        patient_str = json.dumps(p)
        fout.write(f"{patient_str}\n")
    print(counter_num_visits)
    print(c)
    print(non_empty_section_headers)

    fin.close()
    fout.close()


if __name__ == "__main__":
    main()
