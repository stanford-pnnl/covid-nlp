import datetime
import sys
from collections import Counter

import matplotlib.cbook as cbook
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def get_df(path):
    if '.parquet' in path:
        df = pd.read_parquet(path, engine='pyarrow')
    elif '.hdf' in path:
        df = pd.read_hdf(path)
    else:
        print(f"Unhandled path, no matching file extension: {path}")
        sys.exit(1)
    return df


def main():
    # Setup variables
    #path = 'clever_output_test.parquet'
    #path = 'data/prototype_5k_notes2.hdf'
    data_dir = 'data'
    meddra_extractions_dir = f"{data_dir}/medDRA_extractions"
    meddra_extractions_path = f"{meddra_extractions_dir}/meddra_hier_batch3.hdf"
    patients = {}
    num_rows = 0

    # get batch #3 of medDRA extractions from file
    meddra_extractions = get_df(meddra_extractions_path)

    # Get distinct Patient ID values from dataframe
    n_distinct_patid_values = meddra_extractions['patid'].nunique()
    distinct_patid_values = meddra_extractions['patid'].unique().tolist()
    for patid in distinct_patid_values:
        patients[str(patid)] =   
    import pdb; pdb.set_trace()

    print()


if __name__ == '__main__':
    main()
