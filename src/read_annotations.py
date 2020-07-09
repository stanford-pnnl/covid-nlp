import datetime
import sys
from collections import Counter

import matplotlib.cbook as cbook
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def get_annotations(path):
    if '.parquet' in path:
        df = pd.read_parquet(path, engine='pyarrow')
    elif '.hdf' in path:
        df = pd.read_hdf(path)
    else:
        print(f"Unhandled path, no matching file extension: {path}")
        sys.exit(1)
    return df


def main():
    #path = 'clever_output_test.parquet'
    path = 'data/prototype_5k_notes2.hdf'

    df = get_annotations(path)
    #df["note_date"] = pd.to_datetime(df["note_date"])
    df["date"] = pd.to_datetime(df["date"])
    #df.set_index(df["note_date"], inplace=True)
    df.set_index(df["date"], inplace=True)
    # df.groupby([df.note_date.dt.year.rename('year'),
    #                  df.note_date.dt.month.rename('month')])
    month_index = df.index.to_period('M')
    df2 = df.set_index(month_index)
    df3 = df2.sort_index()
    # save to file for DEBUG
    df3.to_csv('prototype_5k_notes2.csv')

    unique_months = [str(x) for x in df3.index.unique()]

    # Set up term counters
    counters = {}
    # Add counter to keep track of count regardless of time period
    counters['all'] = Counter()
    # Add counter for each month key
    for unique_month in unique_months:
        counters[unique_month] = Counter()

    # Count terms by iterating through terms tagged
    for row in df3.itertuples():
        # Make sure polarity is correct
        # if row.polarity != 'POSITIVE':
        #    continue
        # Make sure we are looking at preseent info
        # if row.present != 'PRESENT':
        #    continue
        #term_text_key = row.term_text
        #term_text_key = row.concept_text
        term_text_key = row.SOC_text
        month_key = str(row.Index)
        counters["all"][term_text_key] += 1
        counters[month_key][term_text_key] += 1

    sums = {}
    sums['all'] = {}
    sums['mental_health'] = {}
    labels = []
    num_terms = []
    for unique_month in unique_months:
        labels.append(unique_month)
        # num_terms.append(len(counters[unique_month]))
        # DEBUG
        sum_terms = 0
        for v in counters[unique_month].values():
            sum_terms += v
        num_terms.append(sum_terms)
        sums['all'][unique_month] = sum_terms
        sums['mental_health'][unique_month] = 0
        psychiatric_disorders_count = counters[unique_month].get(
            'Psychiatric disorders', 0)
        sums['mental_health'][unique_month] += psychiatric_disorders_count

    labels_datetimes = [np.datetime64(x) for x in labels]

    decades = mdates.YearLocator(base=10)   # every year
    years = mdates.YearLocator(base=1)   # every year
    months = mdates.MonthLocator()  # every month
    decades_fmt = mdates.DateFormatter('%Y')

    fig, ax = plt.subplots()
    ax.plot(labels_datetimes, num_terms, solid_capstyle='round', marker='.')
    # format the ticks
    ax.xaxis.set_major_locator(decades)
    ax.xaxis.set_major_formatter(decades_fmt)
    ax.xaxis.set_minor_locator(years)

    # format the coords message box
    ax.format_xdata = mdates.DateFormatter('%Y-%m-%d')
    # format to int displayed instead of float
    ax.format_ydata = lambda x: '%1.0i' % x
    ax.grid(True)

    # rotates and right aligns the x labels, and moves the bottom of the
    # axes up to make room for them
    fig.autofmt_xdate()

    # SEt titles
    print(sums)
    plt.show()

    print()


if __name__ == '__main__':
    main()
