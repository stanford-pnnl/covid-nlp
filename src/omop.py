from utils import get_table


def omop_drug_exposure(drug_exposure_dir,
                       prefix='drug_exposure',
                       pattern='',
                       pattern_re='',
                       extension='.csv',
                       use_dask=False,
                       debug=False):
    print("OMOP DRUG_EXPOSURE", flush=True)
    drug_exposure = get_table(drug_exposure_dir,
                              prefix=prefix,
                              pattern=pattern,
                              extension=extension,
                              use_dask=use_dask,
                              debug=debug)
    return drug_exposure


def omop_concept(concept_dir,
                 prefix='concept',
                 pattern='',
                 pattern_re='',
                 extension='.csv',
                 use_dask=False,
                 debug=False):
    print("OMOP CONCEPT", flush=True)
    concept = get_table(concept_dir, prefix=prefix, use_dask=use_dask,
                        debug=debug)
    # FIXME
    # set index to int concept_id
    # concept.set_index('concept_id')
    # Sort by index
    # concept.sort_index(inplace=True)
    return concept
