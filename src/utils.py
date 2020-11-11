import os
import sys
from datetime import date, datetime

import dask.dataframe as dd
import pandas as pd


def get_df(path, use_dask=False, debug=False):
    if use_dask:
        df_lib = dd
    else:
        df_lib = pd

    if '.parquet' in path:
        df = df_lib.read_parquet(path, engine='pyarrow')
    elif '.hdf' in path:
        df = df_lib.read_hdf(path)
    elif '.csv' in path:
        # FIXME, files that are gzipped need to have the correct extension
        # .gz
        df = df_lib.read_csv(path, compression='gzip')
    else:
        print(f"Unhandled path, no matching file extension: {path}")
        sys.exit(1)

    print(f"Successfully read dataframe from path: {path}")
    return df


def get_df_frames(df_frames_dir, use_dask=False, debug=False):
    paths = [path for path in os.listdir(df_frames_dir)]
    paths_full = [os.path.join(df_frames_dir, path) for path in paths]
    # Only load one frame for debug mode
    if debug:
        paths_full = [paths_full[0]]
    print("Attempting to read df from paths: ")
    for path in paths_full:
        print(f"\t{path}")
    df_frames = [get_df(path, use_dask) for path in paths_full]
    df = pd.concat(df_frames, sort=False)
    print("Successfully read dataframe from paths")
    return df


def get_table(table_dir, prefix='', pattern='*', extension='.csv',
              use_dask=False, debug=False):
    print("get_table()")
    if use_dask:
        print("\tUsing dask")
        table_path = f"{table_dir}/{prefix}{pattern}{extension}"
        print(f"\ttable_path: {table_path}")
        df = get_df(table_path, use_dask, debug)
    else:
        print("\tGetting df frames")
        df = get_df_frames(table_dir, use_dask, debug)
    return df


def get_person_ids(df, use_dask=False):
    unique_person_ids = df.person_id.unique()
    if use_dask:
        unique_person_ids = unique_person_ids.compute()

    nunique_person_ids = df.person_id.nunique()
    if use_dask:
        nunique_person_ids = nunique_person_ids.compute()

    print(f"Found {nunique_person_ids} person IDs")
    return unique_person_ids


def get_patient_ids(df, use_dask=False):
    unique_patient_ids = df.patid.unique()
    if use_dask:
        unique_patient_ids = unique_patient_ids.compute()

    nunique_patient_ids = df.patid.nunique()
    if use_dask:
        nunique_patient_ids = nunique_patient_ids.compute()

    print(f"Found {nunique_patient_ids} patient IDs")
    return unique_patient_ids


def get_dates(df, use_dask=False):
    unique_dates = df.date.unique()
    if use_dask:
        unique_dates = unique_dates.compute()

    nunique_dates = df.date.nunique()
    if use_dask:
        nunique_dates = nunique_dates.compute()

    print(f"Found {nunique_dates} dates")
    return unique_dates


def date_obj_to_str(date_obj):
    date_str = date_obj.strftime("%Y-%m-%d")
    return date_str


def date_str_to_obj(date_str):
    date_obj = date.strptime(date_str, "%Y-%m-%d")
    return date_obj


def datetime_obj_to_str(datetime_obj):
    datetime_str = datetime_obj.strftime("%Y-%m-%d")
    return datetime_str


def datetime_str_to_obj(datetime_str):
    datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%d")
    return datetime_obj
