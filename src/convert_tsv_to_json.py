import csv
import json


def main(input_path, output_path):
    output = dict()
    unique_comorbidities = set()
    unique_drugs = set()
    links = []

    with open(input_path, "r") as fin:
        read_tsv = csv.reader(fin, delimiter="\t")
        for row in read_tsv:
            comorbidities = row[0]
            drugs = row[1]
            los_median = row[2]
            unique_comorbidities.add(comorbidities)
            unique_drugs.add(drugs)
            link = dict()
            link["source"] = comorbidities
            link["target"] = drugs
            link["value"] = los_median
            links.append(link)

    nodes = []
    for comorbidity in unique_comorbidities:
        node = dict()
        node["id"] = comorbidity
        node["group"] = 0
        nodes.append(node)

    for drug in unique_drugs:
        node = dict()
        node["id"] = drug
        node["group"] = 1
        nodes.append(node)

    output["nodes"] = nodes
    output["links"] = links

    print(f"Dumping {len(nodes)} nodes and {len(links)} links to {output_path}")
    with open(output_path, "w") as fout:
        json.dump(output, fout)


if __name__ == "__main__":
    repo_dir = "/Users/hamc649/Documents/deepcare/covid-19/covid-nlp"
    script_name = "convert_tsv_to_json"
    script_dir = f"{repo_dir}/{script_name}"
    input_dir = f"{script_dir}/input"
    output_dir = f"{script_dir}/output"

    month = "2020-02"
    month = "2020-03"
    #month = "2020-04"
    #month = "2020-05"
    #month = "2020-06"

    print(f"month: {month}")

    base_path = f"comborbidity_concomitant_drug_interaction_{month}"
    input_path = f"{input_dir}/{base_path}.tsv"
    output_path = f"{output_dir}/{base_path}.json"
    main(input_path, output_path)
