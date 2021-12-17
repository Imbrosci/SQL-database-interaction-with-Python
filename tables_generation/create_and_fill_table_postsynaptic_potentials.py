# -*- coding: utf-8 -*-
"""
Created on Mon Dec  6 15:07:20 2021

Create and fill a table with the SWRs-coupled PSPs parameters from the current
clamp data from neurons from both superficial and deep layers of the entorhinal
cortex.


@author: imbroscb
"""

import psycopg2 as pg2
import pandas as pd

# %% connect with PostgreSQL

# create a connection with PostgreSQL
conn = pg2.connect(database='SWRs_in_entorhinal_cortex', user='your_username',
                   password='your_password')

# create a cursor object
cur = conn.cursor()

# %% create the table

create_table_postsynaptic_potentials = '''
CREATE TABLE postsynaptic_potentials(
    date_of_recording DATE,
    neuron_number SMALLINT,
    slice_number SMALLINT,
    layer VARCHAR(11),
    swrs_incidence DECIMAL(6, 3),
    swrs_uncoupled_amplitude_mv DECIMAL(6, 3),
    swrs_atypical_amplitude_mv DECIMAL(6, 3),
    swrs_standard_amplitude_mv DECIMAL(6, 3),
    psps_SWRs_coupled_percentage DECIMAL(6, 3) NOT NULL,
    psps_coupled_to_atypical_percentage DECIMAL(6, 3),
    psps_coupled_to_standard_percentage DECIMAL(6, 3),
    psps_coupled_to_atypical_amplitude_mv DECIMAL(6, 3),
    psps_coupled_to_standard_amplitude_mv DECIMAL(6, 3),
    vm_justbefore_psps_coupled_to_atypical_mv DECIMAL(6, 3),
    vm_justbefore_psps_coupled_to_standard_mv DECIMAL(6, 3),
    further_comments TEXT,
    PRIMARY KEY(Date_of_recording, Neuron_number)
    );
'''

cur.execute(create_table_postsynaptic_potentials)
conn.commit()

# %% load the file with the values and organize the values as a list of tuples

filepath = 'path/to/file'

df = pd.read_excel(filepath, sheet_name='PSPs_coupled_to_SWRs', header=0)
df = df.sort_values(by='Date_of_recording')

values = []
for c, r in df.iterrows():
    row = []
    for element in r:
        row.append(element)
    values.append(tuple(row))

# %% insert the values into the table

insert_query = ''' INSERT INTO postsynaptic_potentials(
    date_of_recording,
    neuron_number,
    slice_number,
    layer,
    swrs_incidence,
    swrs_uncoupled_amplitude_mv,
    swrs_atypical_amplitude_mv,
    swrs_standard_amplitude_mv,
    psps_swrs_coupled_percentage,
    psps_coupled_to_atypical_percentage,
    psps_coupled_to_standard_percentage,
    psps_coupled_to_atypical_amplitude_mv,
    psps_coupled_to_standard_amplitude_mv,
    vm_justbefore_psps_coupled_to_atypical_mv,
    vm_justbefore_psps_coupled_to_standard_mv,
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

cur.execute('SELECT * FROM postsynaptic_potentials')
description = cur.description

for column_number in range(len(description)):
    column_name = description[column_number][0]
    if (column_name != 'date_of_recording') and (
            column_name != 'neuron_number') and (
            column_name != 'slice_number') and (
            column_name != 'psps_swrs_coupled_percentage'):
        update_query = '''
        UPDATE postsynaptic_potentials
        SET {current_column1} = NULL
        WHERE {current_column2} = 'NaN'
        '''.format(current_column1=column_name, current_column2=column_name)
        cur.execute(update_query)

conn.commit()

# %% close the connection

conn.close()
