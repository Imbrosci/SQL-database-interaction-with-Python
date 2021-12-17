# -*- coding: utf-8 -*-
"""
Created on Fri Dec 17 12:00:45 2021

Create and fill a table with the information about the double patch clamp data
from neurons from both superficial and deep layers of the entorhinal cortex.

@author: imbroscb
"""

import psycopg2 as pg2
import pandas as pd

# %% connect with PostgreSQL

# create a connection with PostgreSQL
conn = pg2.connect(database='SWRs_in_entorhinal_cortex', user='postgres',
                   password='learning_sql')

# create a cursor object
cur = conn.cursor()

# %% create the table

create_table_double_patch = '''
CREATE TABLE double_patch(
    date_of_recording DATE,
    neuron_number SMALLINT,
    pair_number NUMERIC(6, 3),
    layer VARCHAR(11),
    SWRs_coupled_inputs BOOL,
    used_for_cross_corr BOOL,
    pscs_swrs_coupled_percentage DECIMAL(6, 3),
    pscs_coupled_to_atypical_percentage DECIMAL(6, 3),
    pscs_coupled_to_standard_percentage DECIMAL(6, 3),
    cross_corr_peak_atypical DECIMAL(6, 3),
    cross_corr_peak_standard DECIMAL(6, 3),
    further_comments TEXT,
    PRIMARY KEY(Date_of_recording, Neuron_number)
    );
'''

cur.execute(create_table_double_patch)
conn.commit()

# %% load the file with the values and organize the values as a list of tuples

filepath = '''C:/Users/imbroscb/Desktop/science projects/SWRs projects/manuscript 2/analysis for manuscript 2/sql/double_patch_for_SQL.xlsx'''
df = pd.read_excel(filepath, sheet_name='Double_patch_resume', header=0)
df = df.sort_values(by='Date_of_recording')

values = []
for c, r in df.iterrows():
    row = []
    for element in r:
        row.append(element)
    values.append(tuple(row))

# %% insert the values into the table

insert_query = ''' INSERT INTO double_patch(
    date_of_recording,
    neuron_number,
    pair_number,
    layer,
    SWRs_coupled_inputs,
    used_for_cross_corr,
    pscs_swrs_coupled_percentage,
    pscs_coupled_to_atypical_percentage,
    pscs_coupled_to_standard_percentage,
    cross_corr_peak_atypical,
    cross_corr_peak_standard,
    further_comments)
VALUES (%s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s);
'''

try:
    cur.executemany(insert_query, values)
except (Exception, pg2.DatabaseError) as error:
    print(error)

conn.commit()

# %% substitute NaN with NULL values

cur.execute('SELECT * FROM double_patch')
description = cur.description

for column_number in range(len(description)):
    column_name = description[column_number][0]
    if (column_name != 'date_of_recording') and (
            column_name != 'neuron_number') and (
                column_name != 'swrs_coupled_inputs') and (
                    column_name != 'used_for_cross_corr'):
        update_query = '''
        UPDATE double_patch
        SET {current_column1} = NULL
        WHERE {current_column2} = 'NaN'
        '''.format(current_column1=column_name, current_column2=column_name)
        cur.execute(update_query)

conn.commit()

# %% close the connection

conn.close()
