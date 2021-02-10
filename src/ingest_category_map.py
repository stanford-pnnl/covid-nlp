import json
from collections import Counter


def load_category_map(path):
    with open(path, "r") as f:
        category_map = json.load(f)
    return category_map


def get_entries_from_category_map(category_map):
    entries = []
    count = Counter()
    uniq_organs = set()
    uniq_disorders = set()
    for concept_id, value in category_map.items():
        entry = {}
        short_name = value["name"]
        if not short_name:
            continue
        short_name = short_name.replace(" ", "")
        full_name = f"omop.concept.{short_name}"
        entry["name"] = full_name
        # FIXME
        entry["size"] = 100

        entry["imports"] = []
        organ_import = value["organ"]
        if organ_import != "UnknownOrgan":
            organ_import_name = organ_import[1]
            organ_import_name = organ_import_name.replace(" ", "")
            count[organ_import_name] += 1
            organ_import_name_full = f"omop.organ.{organ_import_name}"
            uniq_organs.add(organ_import_name_full)
            entry["imports"].append(organ_import_name_full)

        disorder_import = value["disorder"]
        if disorder_import != "UnknownDisorder":
            disorder_import_name = disorder_import[1]
            disorder_import_name = disorder_import_name.replace(" ", "")
            count[disorder_import_name] += 1
            disorder_import_name_full = f"omop.disorder.{disorder_import_name}"
            uniq_disorders.add(disorder_import_name_full)
            entry["imports"].append(disorder_import_name_full)

        if entry["imports"]:
            entries.append(entry)

    for organ in uniq_organs:
        entry = {}
        entry["name"] = organ
        entry["size"] = 100
        entry["imports"] = []

    for disorder in uniq_disorders:
        entry = {}
        entry["name"] = disorder
        entry["size"] = 100
        entry["imports"] = []
    return entries


def write_entries(path, entries):
    print(f"Writing {len(entries)} entries to {path}")
    with open(path, "w") as f:
        json.dump(entries, f)


def main():
    repo_dir = "/Users/hamc649/Documents/deepcare/covid-19/covid-nlp"
    category_map_path = f"{repo_dir}/category_map.json"
    output_path = "omop.json"
    category_map = load_category_map(category_map_path)
    entries = get_entries_from_category_map(category_map)
    write_entries(output_path, entries)

    print()


if __name__ == "__main__":
    main()
