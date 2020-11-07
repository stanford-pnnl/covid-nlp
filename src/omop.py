from utils import get_table


def omop_drug_exposure(
        drug_exposure_dir, drug_exposure_pattern='drug_exposure',
        use_dask=False, debug=False):
    drug_exposure = \
        get_table(drug_exposure_dir, 'drug_exposure', use_dask, debug)
    return drug_exposure


def omop_concept(concept_dir, use_dask=False, debug=False):
    concept = get_table(concept_dir, 'concept', use_dask, debug)
    # set index to int concept_id
    concept.set_index('concept_id')
    # Sort by index
    concept.sort_index(inplace=True)
    return concept
