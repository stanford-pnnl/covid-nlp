from utils import get_table


def omop_drug_exposure(drug_exposure_dir, prefix='drug_exposure',
                       pattern="", use_dask=False, debug=False):
    print("OMOP DRUG_EXPOSURE")
    # Only look at 10 files instead of 27
    if debug:
        print("\tUsing debug pattern")
        pattern = "00000000000*"
    else:
        print("\tNot using debug pattern")
        pattern = "0000000000*"
    drug_exposure = get_table(drug_exposure_dir,
                              prefix=prefix,
                              pattern=pattern,
                              extension='.csv',
                              use_dask=use_dask,
                              debug=debug)
    return drug_exposure


def omop_concept(concept_dir, use_dask=False, debug=False):
    concept = get_table(concept_dir, prefix='concept', use_dask=use_dask,
                        debug=debug)
    # FIXME
    # set index to int concept_id
    #concept.set_index('concept_id')
    # Sort by index
    #concept.sort_index(inplace=True)
    return concept
