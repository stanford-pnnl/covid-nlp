import json


def load_json(path):
    with open(path, 'r') as f:
        data = json.load(f)
    return data


def search_drug_category(drug_code, drug_labels):
    for drug_label in drug_labels:
        entity_id = drug_label.get('entity_id')
        if not entity_id:
            continue
        if entity_id == drug_code:
            print("Found a drug label match")
            drug_categories = drug_label.get('drug_category')
            if drug_categories:
                if len(drug_categories) > 1:
                    print("Multiple drug categories found, using first")
                    drug_category = drug_categories[0]
                else:
                    drug_category = drug_categories
            return drug_cateogry


def main():
    data_dir = "/Users/hamc649/Documents/deepcare/covid-19/visualization/data/slack"
    patient_kg_path = f"{data_dir}/patient_kg.json"
    patient_kg_labels_path = f"{data_dir}/patient_kg_labels.json"

    patient_kg = load_json(patient_kg_path)
    patient_kg_labels = load_json(patient_kg_labels_path)

    drug_kg_labels = []

    no_space_labels = []
    uniq_drug_categories = set()
    for patient_kg_label in patient_kg_labels:
        label = patient_kg_label['label']
        drug_categories = patient_kg_label.get('drug_category')
        if drug_categories:
            drug_kg_labels.append(patient_kg_label)
            for drug_category in drug_categories:
                uniq_drug_categories.add(drug_category)
            print()
        if ' ' not in label and not label.isnumeric():
            no_space_labels.append(label)
        
        print()

    for patient in patient_kg:
        rxs = patient.get('rx')
        for rx in rxs:
            drug_category = search_drug_category(rx, drug_kg_labels)
        print()

    print()



if __name__ == '__main__':
    main()