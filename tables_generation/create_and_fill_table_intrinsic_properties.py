# -*- coding: utf-8 -*-
"""
Created on Wed Dec  8 18:54:50 2021

Create and fill a table with the intrinsic properties parameters from the
current clamp data of neurons from both superficial and deep layers of the 
entorhinal cortex.


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

create_table_intrinsic_properties = '''
CREATE TABLE intrinsic_properties(
    date_of_recording DATE,
    neuron_number SMALLINT,
    slice_number SMALLINT,
    layer VARCHAR(11),
    input_resistance_mohm DECIMAL(6, 3),
    sag_amplitude_mv DECIMAL(6, 3),
    half_sag_duration_ms DECIMAL(6, 3),
    max_firing_hz DECIMAL(6, 3),
    spike_threshold_5perc_mv DECIMAL(6, 3),
    spike_threshold_3perc_mv DECIMAL(6, 3),
    spike_peak_mv DECIMAL(6, 3),
    spike_half_width_ms DECIMAL(6, 3),
    further_comments TEXT,
    PRIMARY KEY(Date_of_recording, Neuron_number)
    );
'''

cur.execute(create_table_intrinsic_properties)
conn.commit()

# %% load the file with the values and organize the values as a list of tuples

filepath = 'path/to/file'

df = pd.read_excel(filepath, sheet_name='Intrinsic_properties', header=0)
df = df.sort_values(by='Date_of_recording')

values = []
for c, r in df.iterrows():
    row = []
    for element in r:
        row.append(element)
    values.append(tuple(row))

# %% insert the values into the table

insert_query = ''' INSERT INTO intrinsic_properties(
    date_of_recording,
    neuron_number,
    slice_number,
    layer,
    input_resistance_mohm,
    sag_amplitude_mv,
    half_sag_duration_ms,
    max_firing_hz,
    spike_threshold_5perc_mv,
    spike_threshold_3perc_mv,
    spike_peak_mv,
    spike_half_width_ms,
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
        %s);
'''

try:
    cur.executemany(insert_query, values)
except (Exception, pg2.DatabaseError) as error:
    print(error)

conn.commit()

# %% substitute NaN with NULL values

cur.execute('SELECT * FROM intrinsic_properties')
description = cur.description

for column_number in range(len(description)):
    column_name = description[column_number][0]
    if (column_name != 'date_of_recording') and (
            column_name != 'neuron_number') and (
            column_name != 'slice_number'):
        update_query = '''
        UPDATE intrinsic_properties
        SET {current_column1} = NULL
        WHERE {current_column2} = 'NaN'
        '''.format(current_column1=column_name, current_column2=column_name)
        cur.execute(update_query)

conn.commit()

# %% close the connection

conn.close()
