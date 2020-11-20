# library
import datetime
import random

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from dateutil import rrule
from mpl_toolkits.mplot3d import Axes3D


def create_age_data():
    age_data = []
    start_visit_date = datetime.datetime(2020, 1, 1)
    end_visit_date = datetime.datetime(2020, 12, 1)
    age_groups = range(0, 100, 10)
    for month in rrule.rrule(rrule.MONTHLY, dtstart=start_visit_date, until=end_visit_date):
        for age in age_groups:
            num_patients = random.randint(0, 100)
            #data_point = {'Age': age, 'Month': month, 'Patients': num_patients}
            data_point = {'Y': age, 'X': str(month), 'Z': num_patients}
            age_data.append(data_point)
    
    return age_data


def create_age_df():
    data = create_age_data()
    #df = pd.DataFrame(data, index=['Age', 'Month'], columns=['Age', 'Month', 'Patients'])
    df = pd.DataFrame(data, columns=['X', 'Y', 'Z'])
    return df
    
def main():
    # Get the data (csv file is hosted on the web)
    url = 'https://python-graph-gallery.com/wp-content/uploads/volcano.csv'
    data = pd.read_csv(url)

    #age_data = [{''}]
    age_df = create_age_df()
    #age_df = 
    #age_df.columns = ["Age", "Month", "Patient Count"]

    # Transform it to a long format
    #df = data.unstack().reset_index()
    df = age_df
    #df.columns = ["X", "Y", "Z"]

    # And transform the old column name in something numeric
    df['X'] = pd.Categorical(df['X'])
    df['X'] = df['X'].cat.codes
    #df['Y'] = pd.Categorical(df['Y'])
    #df['Y'] = df['Y'].cat.codes

    # Make the plot
    fig = plt.figure()
    ax = fig.gca(projection='3d')
    ax.plot_trisurf(df['Y'], df['X'], df['Z'], cmap=plt.cm.viridis, linewidth=0.2)
    plt.show()

    # to Add a color bar which maps values to colors.
    surf = ax.plot_trisurf(df['Y'], df['X'], df['Z'],
                        cmap=plt.cm.viridis, linewidth=0.2)
    fig.colorbar(surf, shrink=0.5, aspect=5)
    plt.show()

    # Rotate it
    ax.view_init(30, 45)
    plt.show()

    # Other palette
    ax.plot_trisurf(df['Y'], df['X'], df['Z'], cmap=plt.cm.jet, linewidth=0.01)
    plt.show()

if __name__ == '__main__':
    main()