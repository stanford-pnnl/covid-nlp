import json
import os
import sys
from collections import Counter
from pathlib import Path
from datetime import datetime
from dateutil import rrule


def get_patient_visit_id(patient):
    graph_id = patient["graph_id"]
    tokens = graph_id.split("_")
    patient_id = tokens[0]
    visit_id = tokens[1]
    return patient_id, visit_id


def patient_length_of_stay(patient):
    outcome = patient["outcome"]
    length_of_stay = outcome["length_of_stay"]

    return length_of_stay


def create_length_of_stay_map(patient_kg):
    length_of_stay_map = dict()
    unique_length_of_stay_values = set()
    for patient in patient_kg:
        patient_id, visit_id = get_patient_visit_id(patient)
        length_of_stay_map[patient_id] = patient_length_of_stay(patient)
        unique_length_of_stay_values.add(length_of_stay_map[patient_id])
    return length_of_stay_map, unique_length_of_stay_values


def patient_gender(patient):
    attr = patient["attr"]
    gender = attr["gender"]
    return gender


def create_gender_map(patient_kg):
    gender_map = dict()
    unique_gender_values = set()
    for patient in patient_kg:
        patient_id, visit_id = get_patient_visit_id(patient)
        gender_map[patient_id] = patient_gender(patient)
        unique_gender_values.add(gender_map[patient_id])
    return gender_map, unique_gender_values


def patient_is_survivor(patient):
    outcome = patient["outcome"]
    survivor = outcome["survivor"]
    return survivor


def create_survivor_map(patient_kg):
    survivor_map = dict()
    unique_survivor_values = set()
    for patient in patient_kg:
        patient_id, _ = get_patient_visit_id(patient)
        survivor_map[patient_id] = patient_is_survivor(patient)
        unique_survivor_values.add(survivor_map[patient_id])

    return survivor_map, unique_survivor_values


def patient_race(patient):
    attr = patient["attr"]
    race = attr['race']
    return race


def create_race_map(patient_kg):
    race_map = dict()
    unique_races = set()
    for patient in patient_kg:
        patient_id, _ = get_patient_visit_id(patient)
        race_map[patient_id] = patient_race(patient)
        unique_races.add(race_map[patient_id])
    return race_map, unique_races


def load_json(path):
    with open(path, "r") as f:
        data = json.load(f)
    return data


def dump_patient(patient, path):
    with open(path, "a+") as fout:
        patient_str = json.dumps(patient)
        fout.write(f"{patient_str}\n")


def dump_frequencies_quarters(path, quarter_counts, quarters, words):
    print("Dumping frequencies for quarter counts")
    with open(path, "w") as f:
        header = "territory,quarter,profit\n"
        f.write(header)

        for word in sorted(words):
            for quarter in quarters:
                count = quarter_counts[quarter][word]
                freq_line = f"{word},{quarter},{count}\n"
                f.write(freq_line)


def translate_month_name(month):
    if month == "2020-01":
        month_translation = "January 2020"
    if month == "2020-02":
        month_translation = "February 2020"
    if month == "2020-03":
        month_translation = "March 2020"
    if month == "2020-04":
        month_translation = "April 2020"
    if month == "2020-05":
        month_translation = "May 2020"
    if month == "2020-06":
        month_translation = "June 2020"
    if month == "2020-07":
        month_translation = "July 2020"
    if month == "2020-08":
        month_translation = "August 2020"
    if month == "2020-09":
        month_translation = "September 2020"
    if month == "2020-10":
        month_translation = "October 2020"
    if month == "2020-11":
        month_translation = "November 2020"
    if month == "2020-12":
        month_translation = "December 2020"
    return month_translation


def translate_month(month):
    if month == "2020-01":
        month_translation = "Q1 2020"
    if month == "2020-02":
        month_translation = "Q2 2020"
    if month == "2020-03":
        month_translation = "Q3 2020"
    if month == "2020-04":
        month_translation = "Q4 2020"
    if month == "2020-05":
        month_translation = "Q5 2020"
    if month == "2020-06":
        month_translation = "Q6 2020"
    if month == "2020-07":
        month_translation = "Q7 2020"
    if month == "2020-08":
        month_translation = "Q8 2020"
    if month == "2020-09":
        month_translation = "Q9 2020"
    if month == "2020-10":
        month_translation = "Q10 2020"
    if month == "2020-11":
        month_translation = "Q11 2020"
    if month == "2020-12":
        month_translation = "Q12 2020"
    return month_translation


def dump_frequencies_months(path, month_counts, months, words):
    print(f"Dumping frequencies for month counts to {path}")
    with open(path, "w") as f:
        header = "territory,quarter,profit\n"
        f.write(header)

        for word in sorted(words):
            for month in months:
                count = month_counts[month][word]
                #month_translation = translate_month(month)
                # FIXME
                month_translation = translate_month_name(month)
                freq_line = f"{word},{month_translation},{count}\n"
                f.write(freq_line)


def aggregate_quarter_counts(month_counts):
    quarter_counts = dict()
    quarter_counts["Q1 2020"] = Counter()
    quarter_counts["Q2 2020"] = Counter()
    quarter_counts["Q3 2020"] = Counter()
    quarter_counts["Q4 2020"] = Counter()

    for month_key, month_count in month_counts.items():
        if month_key[5:6] == "0":
            month = int(month_key[6:7])
        else:
            month = int(month_key[5:7])

        if month > 0 and month < 4:
            quarter = "Q1 2020"
        elif month > 4 and month < 7:
            quarter = "Q2 2020"
        elif month > 7 and month < 10:
            quarter = "Q3 2020"
        else:
            quarter = "Q4 2020"
        for word, word_count in month_count.items():
            quarter_counts[quarter][word] += word_count
    return quarter_counts


def main(
    input_path,
    output_path,
    output_dir,
    class_of_patients,
    check_gender,
    gender_of_patients,
    gender_map,
    check_length_of_stay,
    length_of_stay_of_patients,
    length_of_stay_map,
    check_race,
    races_of_patients,
    race_map,
    check_survivor_outcome,
    survivor_class_of_patients,
    survivor_map,
    use_first_section,
    use_first_visit,
    drop_text,
):
    # Remove output file
    try:
        os.remove(output_path)
    except OSError:
        print(f"Can't delete {output_path} as it does not exist")

    # We are only counting for the months of 2020 at the moment
    start_visit_date = datetime(2020, 1, 1)
    end_visit_date = datetime(2020, 12, 31)
    visit_dates = []
    month_counts = dict()
    month_keys = []
    for visit_date in rrule.rrule(
        rrule.MONTHLY, dtstart=start_visit_date, until=end_visit_date
    ):
        month_key = visit_date.strftime("%Y-%m")
        month_keys.append(month_key)
        month_counts[month_key] = Counter()
        visit_dates.append(visit_date)

    c = Counter()
    d = Counter()
    p = (
        Counter()
    )  # Counter to see if we have any duplicate patient entries, We DO have multiple entires per patient
    non_empty_section_headers = Counter()
    counter_num_visits = Counter()
    line_num = -1
    with open(input_path, "r") as fin:
        for line in fin:
            line_num += 1
            if line_num % 500 == 0:
                print(f"Processing line no: {line_num}", flush=True)
            try:
                patient = json.loads(line)
            except json.JSONDecodeError:
                c["JSONDecodeError"] += 1
                print(f"line_num: {line_num} failed due to JSONDecodeError")
                continue

            # import pdb;pdb.set_trace()

            # Drop patient ID since it has PHI in it
            if drop_text:
                #patient_id = -9999
                patient_id = patient["patient_id"]
            else:
                patient_id = patient["patient_id"]

            gender = gender_map.get(patient_id)
            if check_gender and gender_of_patients != gender:
                continue  # skip patients that don't have the gender we are counting

            length_of_stay = length_of_stay_map.get(patient_id)
            if check_length_of_stay and length_of_stay_of_patients != length_of_stay:
                continue  # skip patients that don't have the length of stay we are counting

            race = race_map.get(patient_id)
            if check_race and race not in races_of_patients:
                continue  # skip patients that don't have the race we are counting

            survivor = survivor_map.get(patient_id)
            if check_survivor_outcome and survivor_class_of_patients != survivor:
                continue  # skip patients that are in a different class than we are counting


            # Count to see if we have duplicate patients
            p[patient_id] += 1

            c["total_patients"] += 1
            visits = patient["visits"]
            num_visits = len(visits)
            counter_num_visits[str(num_visits)] += 1
            # Skip patients with no visits
            if num_visits < 1:
                continue
            non_empty_visits = []
            # Iterate over a patient's visits
            for visit_i, visit in enumerate(visits):
                c["total_visits"] += 1
                # Only use the first visit
                if use_first_visit and visit_i != 0:
                    continue

                timestamp = visit["timestamp"]
                month_key = timestamp[0:7]
                year_key = timestamp[0:4]
                test_var = "remove"

                # REMOVE temporarily only process 2020 records
                #if year_key != "2020":
                #    continue

                d[month_key] += 1

                # Remote note_ids if we are dropping PHI
                if drop_text:
                    visit["note_id"] = "-9999"
                    #visit["timestamp"] = str(datetime.now())
                    pass

                # Skip empty section data
                non_empty_section_data = []
                for section_i, section in enumerate(visit["section_data"]):
                    c["total_sections"] += 1
                    # Only use the first section
                    if use_first_section and section_i != 0:
                        continue

                    risk_factor_entity_results = section.get("risk_factor_entity")
                    snomed_entity_results = section.get("snomed_entity")

                    if not risk_factor_entity_results and not snomed_entity_results:
                        c["empty_sections"] += 1
                        continue

                    test_var = "remove"

                    if risk_factor_entity_results:
                        risk_factor_entities = [
                            x["entity"] for x in risk_factor_entity_results
                        ]
                        if len(risk_factor_entities) > 1:
                            test_var = "remove"
                        for entity in risk_factor_entities:
                            try:
                                month_counts[month_key][entity] += 1
                            except KeyError:
                                pass

                        test_var = "remove"
                    # entity_extraction_results = section['entity_extraction_results']

                    # Drop 'text' from entity_extraction_results items
                    if drop_text:
                        # for entity_extraction_dresult in entity_extraction_results:
                        #    entity_extraction_result['text'] = 'REMOVED'
                        if risk_factor_entity_results:
                            for result in risk_factor_entity_results:
                                if result.get("text"):
                                    result["text"] = "REMOVED"
                        if snomed_entity_results:
                            for result in snomed_entity_results:
                                if result.get("text"):
                                    result["text"] = "REMOVED"

                    non_empty_section_headers[section["section_header"]] += 1
                    c["non_empty_sections"] += 1

                    # Drop 'section_test'
                    if drop_text:
                        if section.get("section_text"):
                            section["section_text"] = "REMOVED"
                    non_empty_section_data.append(section)
                # replace section data with non empty section data
                if not non_empty_section_data:
                    c["empty_visits"] += 1
                    continue
                c["non_empty_visits"] += 1
                visit["section_data"] = non_empty_section_data
                non_empty_visits.append(visit)

            if not non_empty_visits:
                c["empty_patients"] += 1
                continue
            c["non_empty_patients"] += 1
            patient["visits"] = non_empty_visits
            patient["patient_id"] = str(patient_id)

            dump_patient(patient, output_path)
            c["dumped_patients"] += 1
    print(c, flush=True)
    print(d, flush=True)
    print()

    if check_survivor_outcome:
        survivor_str = "survivor_" if survivor_class_of_patients else "non-survivor_"
    else:
        survivor_str = ""

    if check_length_of_stay:
        length_of_stay_str = f"{length_of_stay_of_patients}_"
    else:
        length_of_stay_str = ""

    if check_gender:
        gender_str = f"{gender_of_patients}_"
    else:
        gender_str = ""

    if check_race:
        #race_of_patients_formatted = race_of_patients.lower()
        #race_of_patients_formatted = race_of_patients_formatted.replace(' ', '_')
        #race_str = f"{race_of_patients_formatted}_"
        if len(races_of_patients) > 1:
            race_str = "non-whites_"
        else:
            race_str = "whites_"
    else:
        race_str = ""

    unique_path = f"{gender_str}{length_of_stay_str}{race_str}{survivor_str}{class_of_patients}"


    frequencies_months_path = f"{output_dir}/months_{unique_path}.csv"
    months_unique_words = set()

    for month, month_count in month_counts.items():
        for word, word_count in month_count.items():
            months_unique_words.add(word)

    quarter_counts = aggregate_quarter_counts(month_counts)

    # Dump month counts to file
    # frequencies = []

    frequencies_quarters_path = f"{output_dir}/quarters_{unique_path}.csv"
    quarters_unique_words = set()

    for quarter, quarter_count in quarter_counts.items():
        for word, word_count in quarter_count.items():
            quarters_unique_words.add(word)
    #        item = (word, quarter, word_count)
    #        frequencies.append(item)

    quarters = ["Q1 2020", "Q2 2020", "Q3 2020", "Q4 2020"]

    # We are not including Jan, February, October, November, December because the data seems empty
    months = [
        "2020-02",
        "2020-03",
        "2020-04",
        "2020-05",
        "2020-06",
        "2020-07",
        "2020-08",
        "2020-09",
    ]

    #dump_frequencies_months(
    #    frequencies_months_path, month_counts, months, months_unique_words
    #)
    #dump_frequencies_quarters(
    #    frequencies_quarters_path, quarter_counts, quarters, quarters_unique_words
    #)
    print()


if __name__ == "__main__":
    use_local = True
    if use_local:
        repo_dir = "/Users/hamc649/Documents/deepcare/covid-19/covid-nlp"
        # base_path = 'covid_like_patients_entity_batch000'
        # base_path = 'covid_like_patients_entity-first_visit'
        # base_path = 'covid_like_patients_entity-first_section'
        # siyi's latest format
        # base_path = 'entity_risk_by_patients_processed_covid_admission_notes'
        # base_path = (
        #    "entity_risk_by_patients_processed_covidlike_admission_notes_batch014"
        # )
    else:
        repo_dir = "/home/colbyham/covid-nlp"
        base_path = "covid_like_patients_entity"

    n_partitions = 2
    n_patients_per_partition = 1000
    extension = "jsonl"
    # script_name = 'drop_text'
    script_name = "ingest_jsonl"
    input_dir = f"{repo_dir}/{script_name}/input"
    output_dir = f"{repo_dir}/{script_name}/output"
    slack_dir = f"/Users/hamc649/Documents/deepcare/covid-19/visualization/data/slack"

    # Make sure output directory is created
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # only use the first visit
    use_first_visit = False
    print(f"use_first_visit: {use_first_visit}")

    # only use the first section for each visit
    use_first_section = False
    print(f"use_first_section: {use_first_section}")

    # drop text
    drop_text = False
    print(f"drop_text: {drop_text}")

    # Build output suffix based on bools
    output_suffix = ""
    if use_first_section:
        output_suffix += "-first_section"
    if use_first_visit:
        output_suffix += "-first_visit"
    if drop_text:
        output_suffix += "-drop_text"

    use_partitions = False
    print(f"use_partitions: {use_partitions}")

    length_of_stay_of_patients = 'los_1_wk'
    #length_of_stay_of_patients = 'los_2_wks'
    #length_of_stay_of_patients = 'los_2-4_wks'
    #length_of_stay_of_patients = "los_gt_4_wks"

    class_of_patients = "covid"
    print(f"class_of_patients: {class_of_patients}")

    gender_of_patients = 'FEMALE'
    print(f"gender_of_patients: {gender_of_patients}")

    survivor_class_of_patients = False
    print(f"survivor_class_of_patients: {survivor_class_of_patients}")

    races_of_patients = ['American Indian or Alaska Native', 'Asian', 'Black or African American', 'Native Hawaiian or Other Pacific Islander', 'Other', 'Other, Hispanic', 'Unkown', 'Patient Refused'] 
    #races_of_patients = ['White']
    #race_of_patients = 'Asian'
    #race_of_patients = 'Black or African American'
    #race_of_patients = 'Other'
    #race_of_patients = 'Patient Refused'
    #race_of_patients = 'White'
    print(f"races_of_patients: {races_of_patients}")

    if class_of_patients == "covid":
        #base_path = "entity_risk_by_patients_processed_covid_admission_notes"
        base_path = "entity_risk_by_patients_processed_note"
    elif class_of_patients == "covidlike":
        base_path = "entity_risk_by_patients_processed_covidlike_admission_notes"

    input_path = f"{input_dir}/{base_path}.jsonl"
    output_path = f"{output_dir}/{base_path}{output_suffix}.jsonl"
    # frequencies_path = f"{output_dir}/freq-{base_path}.csv"
    print(f"\tinput_path: {input_path}", flush=True)
    print(f"\toutput_path: {output_path}", flush=True)
    # sys.exit(0)
    
    patient_kg_path = f"{slack_dir}/patient_kg.json"
    patient_kg_labels_path = f"{slack_dir}/patient_kg_labels.json"

    patient_kg = load_json(patient_kg_path)
    patient_kg_labels = load_json(patient_kg_labels_path)

    check_gender = False
    print(f"check_gender: {check_gender}")
    gender_map, unique_gender_values = create_gender_map(patient_kg)

    check_length_of_stay = False
    print(f"check_length_of_stay: {check_length_of_stay}")
    length_of_stay_map, unique_length_of_stay_values = create_length_of_stay_map(
        patient_kg
    )

    check_survivor_outcome = False
    print(f"check_survivor_outcome: {check_survivor_outcome}")
    survivor_map, unique_survivor_values = create_survivor_map(patient_kg)

    check_race = False
    print(f"check_race: {check_race}")
    race_map, unique_race_values = create_race_map(patient_kg)

    main(
        input_path,
        output_path,
        output_dir,
        class_of_patients,
        check_gender,
        gender_of_patients,
        gender_map,
        check_length_of_stay,
        length_of_stay_of_patients,
        length_of_stay_map,
        check_race,
        races_of_patients,
        race_map,
        check_survivor_outcome,
        survivor_class_of_patients,
        survivor_map,
        use_first_section,
        use_first_visit,
        drop_text,
    )
