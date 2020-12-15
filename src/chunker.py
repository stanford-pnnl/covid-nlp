import json
import re
from copy import deepcopy
from io import StringIO


def flush_patient_data(patient_buf, path, file_id):
    out_path = '%s-part-%d.json' % (path, file_id)
    print('Writing out: %s\n' % out_path)
    with open(out_path, 'w') as f_out:
        patient_line_full = ''.join(patient_buf)
        #patient_id = patient_line_full[1:9]
        patient_line = patient_line_full[15:]
        patient = json.loads(patient_line)
        other_p_match_re = [x for x in patient_buf if re.match(r"[^\S]'[\d]{8}':[^\S]{\\n", x)]
        #for json_line in patient_buf:
        #    f_out.write('%s' % json_line)
    return

def format_patient(patient_lines, first_line, last_line, path, file_id):
    patient_lines[-1] = patient_lines[-1].replace('},', '}')
    patient_str = ''.join(patient_lines)
    patient_str_full = f"{first_line}{patient_str}{last_line}"
    patient_file = StringIO(patient_str_full)
    patient = json.load(patient_file)
    print()
    return patient


def chunk_big_json(path, num_patients_per_file):
    #fill_patient_buf = False
    #num_patients = 0
    #last_line = ''

    patient_id_pattern = re.compile(r'^ {4}"\d{8}": {\n$')
    file_id = 0
    patients = []
    patient = []
    first_line = ''
    last_line = ''
    with open(path) as f:
        for line in f:
            if not first_line:
                first_line = line
                continue
            patient_id_match = patient_id_pattern.match(line)
            # Found patient line
            if patient_id_match:
                if patient:
                    #patient.insert(0, first_line)
                    patients.append(deepcopy(patient))
                    patient.clear()
                    #flush_patient_data(patient_buf, path, file_id)
            patient.append(line)
            last_line = line
            continue
            # if line.find('\"visits\": [') != -1:
            #     num_patients += 1
            #     if fill_patient_buf == False:
            #         patient_buf.append(last_line)
            #         fill_patient_buf = True
            #     else:
            #         # Remove the last line and flush current patient
            #         if num_patients == num_patients_per_file:
            #             patient_buf.pop()
            #             if patient_buf[-1].find('},') != -1:
            #                 patient_buf[-1] = patient_buf[-1].replace('},', '}')
            #             flush_patient_data(patient_buf, path, file_id)
            #             file_id += 1
            #             num_patients = 0
            #             patient_buf = [last_line]
            # if fill_patient_buf == True:
            #     patient_buf.append(line)
            # last_line = line
        if patient:
            print()
            # Remove the last line for the entire patients dump
            patient.pop()
            #patient.insert(0, first_line)
            patients.append(deepcopy(patient))
            patient.clear()
            patient.append(line)
            #flush_patient_data(patient_buf, path, file_id)
        print()
        for patient in patients:
            #flush_patient_data(patient, path, file_id)
            format_patient(patient, first_line, last_line, path, file_id)
        #if len(patient_buf) > 0:
        #    # This is the last file, so pop the last line with } from original file
        #    patient_buf.pop()
        #    flush_patient_data(patient_buf, path, file_id)


if __name__ == '__main__':
    repo_dir = '/Users/hamc649/Documents/deepcare/covid-19/covid-nlp'
    input_path = 'input/covid_like_patients_entity_batch000.json'
    chunk_big_json(input_path, 1)
