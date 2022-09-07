import csv
from collections import Counter


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
    print("Dumping frequencies for month counts")
    with open(path, "w") as f:
        header = "territory,quarter,profit\n"
        f.write(header)

        for word in sorted(words):
            for month in months:
                count = month_counts[month][word]
                month_translation = translate_month(month)
                freq_line = f"{word},{month_translation},{count}\n"
                f.write(freq_line)


def month_counts_comorbidity(input_path, base_path, output_dir):
    month_counts = dict()
    months = [
        "2020-01",
        "2020-02",
        "2020-03",
        "2020-04",
        "2020-05",
        "2020-06",
        "2020-07",
        "2020-08",
        "2020-09",
        "2020-10",
        "2020-11",
        "2020-12",
    ]
    for month in months:
        month_counts[month] = Counter()

    with open(input_path, 'r') as f:
        read_tsv = csv.reader(f, delimiter="\t")
        for row in read_tsv:
            month = row[0]
            attribute = row[1]
            count = row[2]
            month_counts[month][attribute] = count

    print("Dumping month counts for comorbidity related attributes")
    months_unique_words = set()
    for month, month_count in month_counts.items():
        for word, word_count in month_count.items():
            months_unique_words.add(word)

    output_path = f"{output_dir}/{base_path}.csv"
    dump_frequencies_months(
        output_path, month_counts, months, months_unique_words
    )

        
def month_counts_age_race_sex(input_path, base_path, output_dir):
    month_counts_age = dict()
    month_counts_race = dict()
    month_counts_sex = dict()

    months = [
        "2020-01",
        "2020-02",
        "2020-03",
        "2020-04",
        "2020-05",
        "2020-06",
        "2020-07",
        "2020-08",
        "2020-09",
        "2020-10",
        "2020-11",
        "2020-12",
    ]
    for month in months:
        month_counts_age[month] = Counter()
        month_counts_race[month] = Counter()
        month_counts_sex[month] = Counter()

    with open(input_path, 'r') as f:
        read_tsv = csv.reader(f, delimiter="\t")
        for row in read_tsv:
            month = row[0]
            attribute = row[1]
            count = row[2]
            if 'age' in attribute:
                month_counts_age[month][attribute] = count
            elif 'race' in attribute:
                month_counts_race[month][attribute] = count
            elif 'sex' in attribute:
                month_counts_sex[month][attribute] = count
                


    print("Dumping month counts for age related attributes")
    months_unique_words_age = set()
    for month, month_count in month_counts_age.items():
        for word, word_count in month_count.items():
            months_unique_words_age.add(word)

    output_path_age = f"{output_dir}/{base_path}-age.csv"
    dump_frequencies_months(
        output_path_age, month_counts_age, months, months_unique_words_age
    )

    print("Dumping month counts for race related attributes")
    months_unique_words_race = set()
    for month, month_count in month_counts_race.items():
        for word, word_count in month_count.items():
            months_unique_words_race.add(word)

    output_path_race = f"{output_dir}/{base_path}-race.csv"
    dump_frequencies_months(
        output_path_race, month_counts_race, months, months_unique_words_race
    )

    print("Dumping month counts for sex related attributes")
    months_unique_words_sex = set()
    for month, month_count in month_counts_sex.items():
        for word, word_count in month_count.items():
            months_unique_words_sex.add(word)

    output_path_sex = f"{output_dir}/{base_path}-sex.csv"
    dump_frequencies_months(
        output_path_sex, month_counts_sex, months, months_unique_words_sex
    )





if __name__ == '__main__':
    repo_dir = "/Users/hamc649/Documents/deepcare/covid-19/covid-nlp"
    script_name = 'convert_tsv_to_csv'
    script_dir = f"{repo_dir}/{script_name}"
    input_dir = f"{script_dir}/input"
    output_dir = f"{script_dir}/output"
    #base_path = "los_by_month_severity_true"
    #input_path = f"{input_dir}/{base_path}.tsv"
    #month_counts_age_race_sex(input_path, base_path, output_dir)

    base_path = "los_by_month_comorbidity"
    input_path = f"{input_dir}/{base_path}.tsv"
    month_counts_comorbidity(input_path, base_path, output_dir)
