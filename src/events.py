from collections import Counter
from datetime import datetime
from typing import Dict, Set

import dask.dataframe as dd
import pandas as pd

from data_schema import Event
from patient_db import PatientDB


def get_diagnosis_events_distress(patients: PatientDB, row, date_str,
                                  patient_id):
    concept_text_distress = ['Emotional distress', 'emotional distress']

    PT_text_distress = ['Emotional distress']
    PT_text = row.PT_text
    concept_text = row.concept_text

    # Check for distress
    distress_diagnosis_event_found = False
    diagnosis_long_name = ''
    if PT_text in PT_text_distress:
        distress_diagnosis_event_found = True
        diagnosis_long_name = PT_text
    if concept_text in concept_text_distress:
        distress_diagnosis_event_found = True
        diagnosis_long_name = concept_text

    # If we find a distress diagnosis event, create it
    if distress_diagnosis_event_found:
        distress_diagnosis_event = \
            Event(chartdate=date_str,
                  visit_id=date_str,
                  patient_id=patient_id)
        distress_diagnosis_event.diagnosis_role(
            diagnosis_name='Distress',
            diagnosis_long_name=diagnosis_long_name)
        distress_diagnosis_event.add_meddra_roles(row)
        patients.add_event(distress_diagnosis_event)
    return distress_diagnosis_event_found


def get_diagnosis_events_insomnia(patients: PatientDB, row, date_str,
                                  patient_id):
    concept_text_insomnia = [
        'Behavorial insomnia of childhood'
        'Chronic insomnia',
        'Initial insomnia',
        'Primary insomnia',
        'chronic insomnia',
        'insomnia',
        'primary insomnia',
        'psychological insomnia']

    PT_text_insomnia = [
        'Behavioural insomnia of childhood',
        'Initial insomnia',
        'Middle insomnia',
        'Psychophysiologi insomnia',
        'Terminal insomnia']

    # Match(SOC="*", HLGT="cardiac_valve_disorders", HLT="", PT="")
    PT_text = row.PT_text
    concept_text = row.concept_text

    # Check for insomnia
    insomnia_diagnosis_event_found = False
    diagnosis_long_name = ''
    if PT_text in PT_text_insomnia:
        insomnia_diagnosis_event_found = True
        diagnosis_long_name = PT_text
    if concept_text in concept_text_insomnia:
        insomnia_diagnosis_event_found = True
        diagnosis_long_name = concept_text

    # If we find an insomnia diagnosis event, create it
    if insomnia_diagnosis_event_found:
        insomnia_diagnosis_event = \
            Event(chartdate=date_str,
                  visit_id=date_str,
                  patient_id=patient_id)
        insomnia_diagnosis_event.diagnosis_role(
            diagnosis_name='Insomnia',
            diagnosis_long_name=diagnosis_long_name)
        insomnia_diagnosis_event.add_meddra_roles(row)
        patients.add_event(insomnia_diagnosis_event)

    return insomnia_diagnosis_event_found


def get_diagnosis_events_anxiety(patients: PatientDB, row, date_str,
                                 patient_id):
    concept_text_anxiety = [
        'Adjustment disorder with anxiety',
        'Chronic anxiety',
        'Generalized anxiety disorder',
        'Situational anxiety',
        'Social anxiety disorder',
        'adjustment disorder with anxiety',
        'anxiety',
        'anxiety attack',
        'anxiety disorder',
        'anxiety symptoms',
        'chronic anxiety',
        'generalized anxiety disorder',
        'separation anxiety',
        'situational anxiety',
        'social anxiety disorder']

    PT_text_anxiety = [
        'Adjustment disorder with anxiety',
        'Generalised anxiety disorder',
        'Illness anxiety disorder',
        'Separation anxiety disorder',
        'Social anxiety disorder']

    PT_text = row.PT_text
    concept_text = row.concept_text

    # Check for anxiety
    anxiety_diagnosis_event_found = False
    diagnosis_long_name = ''
    if PT_text in PT_text_anxiety:
        anxiety_diagnosis_event_found = True
        diagnosis_long_name = PT_text
    if concept_text in concept_text_anxiety:
        anxiety_diagnosis_event_found = True
        diagnosis_long_name = concept_text

    # If we find an anxiety diagnosis event, create it
    if anxiety_diagnosis_event_found:
        anxiety_diagnosis_event = Event(chartdate=date_str, visit_id=date_str,
                                        patient_id=patient_id)
        anxiety_diagnosis_event.diagnosis_role(
            diagnosis_name='Anxiety', diagnosis_long_name=diagnosis_long_name)
        anxiety_diagnosis_event.add_meddra_roles(row)

        patients.add_event(anxiety_diagnosis_event)

    return anxiety_diagnosis_event_found


def get_diagnosis_events_depression(patients: PatientDB,
                                    row,
                                    date_str,
                                    patient_id):
    concept_text_depression = [
        'Anxious depression',
        'Bipolar depression',
        'Chronic depression',
        'Major depression',
        'Post stroke depression',
        'Postpartum depression',
        'Reactive depression',
        'ST segment depression',
        'bipolar depression',
        'chronic depression',
        'depression',
        'depression nos',
        'major depression',
        'manic depression',
        'mood depression',
        'post stroke depression',
        'postpartum depression',
        'reactive depression',
        'suicidal depression']

    PT_text_depression = [
        'Major depression',
        'Perinatal depression',
        'Post stroke depression']
    PT_text = row.PT_text
    concept_text = row.concept_text

    # Check for 'depression'
    depression_diagnosis_event_found = False
    diagnosis_long_name = ''
    if PT_text in PT_text_depression:
        depression_diagnosis_event_found = True
        diagnosis_long_name = PT_text
    if concept_text in concept_text_depression:
        depression_diagnosis_event_found = True
        diagnosis_long_name = concept_text

    # If we find a depression diagnosis event, create it
    if depression_diagnosis_event_found:
        # Create a basic event
        depression_diagnosis_event = Event(
            chartdate=date_str,
            visit_id=date_str,
            patient_id=patient_id)
        # Add diagnosis attributes
        depression_diagnosis_event.diagnosis_role(
            diagnosis_name='Depression',
            diagnosis_long_name=diagnosis_long_name)
        # Add MEDDRA attributes
        depression_diagnosis_event.add_meddra_roles(row)
        patients.add_event(depression_diagnosis_event)

    return depression_diagnosis_event_found


def get_distinct_column_values(df, output_dir, keys, use_dask=False):
    print("Getting distinct values from columns and dumping to files")
    for key in sorted(keys):
        try:
            distinct_column_values = list(df[key].unique())  # .tolist()
        except KeyError:
            print(f"\t{key}, not in dataframe, skipping...")
            continue
        except TypeError:
            print(f"\t{key}, TypeError, skipping...")
            continue
        except AttributeError:
            print(f"\t{key}, AttributeError, skipping...")
            continue
        except NotImplementedError:
            print(f"\t{key}, NotImplementedError, skipping...")
            continue

        output_path = f"{output_dir}/{key}.txt"
        print(f"\t{key}, dumping {len(distinct_column_values)} distinct values"
              f" to {output_path}")
        with open(output_path, 'w') as f:
            try:
                distinct_column_values = sorted(distinct_column_values)
            except TypeError:
                pass
            for distinct_column_value in distinct_column_values:
                f.write(f"{distinct_column_value}\n")


def get_diagnosis_events(patients: PatientDB, df):
    print("Getting diagnosis events...")
    columns: Dict[str, Counter] = dict()
    column_names = df.columns.tolist()
    for column in column_names:
        columns[column] = Counter()
    print(df.head())

    # See if you can find a generalizable way to iterate without using
    # itterrows
    # temporarily using to satisfy unkown columns addition to roles

    # FIXME, only look at 1000000 rows
    i_max = 10000
    print(f"Limiting iteration of dataframe to a maximum of {i_max} rows")
    for i, row in enumerate(df.itertuples()):
        if i % (i_max / 10) == 0:
            now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"{now_str} Tuple: {i}/{i_max}")
        if i >= i_max:
            break
        #import pdb;pdb.set_trace()
        date_str = row.date
        patient_id = str(row.patid)

        # Meddra column value counters
        #count_column_values(row, columns)

        # Check for different types of diagnosis events
        found_depression = \
            get_diagnosis_events_depression(
                patients, row, date_str, patient_id)
        found_anxiety = \
            get_diagnosis_events_anxiety(patients, row, date_str, patient_id)
        found_insomnia = \
            get_diagnosis_events_insomnia(patients, row, date_str, patient_id)
        found_distress = \
            get_diagnosis_events_distress(patients, row, date_str, patient_id)

        # If we don't find a mental health symptom assume we have found
        # a diagnosis event/symptom without match
        found_any_events = \
            any([found_depression,
                 found_anxiety,
                 found_insomnia,
                 found_distress])

        if found_any_events:
            continue
        # Add meddra event
        meddra_event = \
            Event(chartdate=date_str, visit_id=date_str, patient_id=patient_id)
        meddra_event.meddra_role(row)
        patients.add_event(meddra_event)

    #print(f"columns: {columns}")
    #print("Top 10 diagnosis ")
    #import pdb;pdb.set_trace()


def get_medication_events(patients: PatientDB, concept_df, df, use_dask=False):
    print("Getting medication events...", flush=True)
    columns: Dict[str, Counter] = dict()
    column_names = df.columns.tolist()
    for column in column_names:
        columns[column] = Counter()
    print(df.head())

    #new_df = df.join(concept_df.set_index('concept_id'),
    # n='drug_concept_id', how="left", ruffix="")
    if use_dask:
        df_lib = pd
    else:
        df_lib = dd

    #new_df = df_lib.merge(
    #    df, concept_df, how="left", left_on="drug_concept_id",
    #    right_on="concept_id", suffixes=('', '_right'))
    # Large to Small Join
    # Ensure that the smaller concept table can fit into a single
    # partititon of memory
    if use_dask:
        concept_df = concept_df.repartition(npartitions=1)
    new_df = df.merge(concept_df, how="left", left_on="drug_concept_id",
                      right_on="concept_id", suffixes=('', '_right'))
    #import pdb;pdb.set_trace()

    drug_concept_class_ids: Set[str] = set()
    accepted_drug_concept_class_ids = set()
    #accepted_drug_concept_class_ids.add('Prescription Drug')
    #accepted_drug_concept_class_ids.add('Ingredient')
    #accepted_drug_concept_class_ids.add('CVX')
    #accepted_drug_concept_class_ids.add('Undefined')
    #accepted_drug_concept_class_ids.add('Drug Product')
    #accepted_drug_concept_class_ids.add('Branded Drug')
    #accepted_drug_concept_class_ids.add('Branded Drug Form')
    accepted_drug_concept_class_ids.add('Clinical Drug')
    #accepted_drug_concept_class_ids.add('Clinical Drug Comp')
    # Quantity first in string
    #accepted_drug_concept_class_ids.add('Quant Clinical Drug')

    # FIXME
    # get medication events from drug_exposure table
    i_max = 10000
    print(f"Limiting iteration of dataframe to a maximum of {i_max} rows")
    for i, row in enumerate(new_df.itertuples()):
        if i % (i_max / 10) == 0:
            now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"{now_str} Tuple: {i}/{i_max}")
        if i >= i_max:
            break
        # FIXME, should events be required to have a single date if
        # they are more of an event range?
        patient_id = str(row.person_id)
        date_str = row.drug_exposure_start_DATE

        #import pdb;pdb.set_trace()

        #dose_unit_concept_id = row.dose_unit_concept_id
        #drug_concept_id = row.drug_concept_id

        # Skip rows not in accepted concept class IDs
        # (prescriptions approximate)
        drug_concept_class_id = row.concept_class_id
        if drug_concept_class_id not in accepted_drug_concept_class_ids:
            continue

        drug_concept_name = row.concept_name
        #drug_concept_name = get_concept_name(concept_df, drug_concept_id)
        #if drug_concept_class_id != 'Clinical Drug':
        #    print(f"drug_concept_class_id: {drug_concept_class_id}, "
        #          f"drug_concept_name: {drug_concept_name}")

        #drug_source_concept_id = row.drug_source_concept_id
        #drug_source_concept_name = \
        #    get_concept_name(concept_df, drug_source_concept_id)

        # We could drop 'Patient Self-Reported Medication'?
        # The Drug era categories aren't clear
        #drug_type_concept_id = row.drug_type_concept_id
        #drug_type_concept_name = \
        #    get_concept_name(concept_df, drug_type_concept_id)

        #route_concept_id = row.route_concept_id
        #route_concept_name = get_concept_name(concept_df, route_concept_id)

        #import pdb;pdb.set_trace()

        # Make sure it is a drug event by checking the concept table
        drug_exposure_event = \
            Event(chartdate=date_str, visit_id=date_str, patient_id=patient_id)
        drug_exposure_event.drug_exposure_role(row, drug_concept_name)
        # TODO, convert drug_exposure events to medication events?
        patients.add_event(drug_exposure_event)
    print(f"drug_concept_class_ids: {drug_concept_class_ids}")


def get_events(patients: PatientDB, omop_concept_df, diagnosis_df,
               omop_drug_exposure_df, use_dask=False):
    print("Getting events...")
    print("Getting diagnosis events")
    get_diagnosis_events(patients, diagnosis_df)

    print("Getting medication events")
    get_medication_events(patients, omop_concept_df, omop_drug_exposure_df,
                          use_dask=use_dask)
