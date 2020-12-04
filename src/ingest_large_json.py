import json
import ijson

def main():
    repo_dir = '/Users/hamc649/Documents/deepcare/covid-19/covid-nlp'
    patients_batch_path = f"{repo_dir}/covid_like_patients_entity_batch000.json"
    output_path = f"{repo_dir}/patients_batch000.jsonl"
    f = open(patients_batch_path, 'r')
    fout = open(output_path, 'w')
    batch = ijson.items(f, '')
    print()
    num_patients = 0
    patients = []
    for patient_batch in batch:
        for k, v in patient_batch.items():
            print(k)
            v['patient_id'] = str(k)
            num_patients += 1
            patient_str = json.dumps(v)
            fout.write(f"{patient_str}\n")
        print()
    f.close()
    fout.close()


if __name__ == "__main__":
    main()
