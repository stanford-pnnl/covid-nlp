def flush_patient_data(patient_buf, path, file_id):
    out_path = '%s-part-%d.json' % (path, file_id)
    print('Writing out: %s\n' % out_path)
    patient_buf = []
    with open(out_path, 'w') as f_out:
        patient_line = ''.join(patient_buf)
        for json_line in patient_buf:
            f_out.write('%s' % json_line)
    return


def chunk_big_json(path, num_patients_per_file):
    with open(path) as f:
        num_patients = 0
        file_id = 0
        last_line = ''
        patient_buf = []
        fill_patient_buf = False
        for line in f:
            line = line.strip()
            if line.find('\"visits\": [') != -1:
                num_patients += 1
                if fill_patient_buf == False:
                    patient_buf.append(last_line)
                    fill_patient_buf = True
                else:
                    # Remove the last line and flush current patient
                    if num_patients == num_patients_per_file:
                        patient_buf.pop()
                        if patient_buf[-1].find('},') != -1:
                            patient_buf[-1] = patient_buf[-1].replace('},', '}')
                        flush_patient_data(patient_buf, path, file_id)
                        file_id += 1
                        num_patients = 0
                        patient_buf = [last_line]
            if fill_patient_buf == True:
                patient_buf.append(line)
            last_line = line
        if len(patient_buf) > 0:
            # This is the last file, so pop the last line with } from original file
            patient_buf.pop()
            flush_patient_data(patient_buf, path, file_id)


if __name__ == '__main__':
    repo_dir = '/Users/hamc649/Documents/deepcare/covid-19/covid-nlp'
    input_path = 'input/covid_like_patients_entity_batch000.json'
    chunk_big_json(input_path, 500)
