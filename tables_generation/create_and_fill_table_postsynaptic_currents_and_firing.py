# -*- coding: utf-8 -*-
"""
Created on Fri Dec 10 11:27:03 2021

Create and fill a table with the SWRs-coupled PSCs parameters from the voltage
clamp data and the SWRs-coupled firing paramenters from the current clamp data
from neurons from both superficial and deep layers of the entorhinal cortex.

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

# the data type for increased firing and decreased firing is set to NUMERIC
# where (1 = True, 0 = False)

create_table_postsynaptic_currents_and_firing = '''
CREATE TABLE postsynaptic_currents_and_firing(
    date_of_recording DATE,
    neuron_number SMALLINT,
    slice_number SMALLINT,
    layer VARCHAR(11),
    psc_peaktime_ms DECIMAL(6,3),
    reversal_potential_at_peaktime_mv DECIMAL(6,3),
    increased_firing NUMERIC (1,0),
    decreased_firing NUMERIC (1,0),
    further_comments TEXT,
    PRIMARY KEY(Date_of_recording, Neuron_number)
    );
'''

cur.execute(create_table_postsynaptic_currents_and_firing)
conn.commit()

# %% load the file with the values and organize the values as a list of tuples

filepath = 'path/to/file'

df = pd.read_excel(filepath, sheet_name='voltage_clamp_data', header=0)
df = df.sort_values(by='Date_of_recording')

values = []
for c, r in df.iterrows():
    row = []
    for element in r:
        row.append(element)
    values.append(tuple(row))

# %% insert the values into the table

insert_query = ''' INSERT INTO postsynaptic_currents_and_firing(
    date_of_recording,
    neuron_number,
    slice_number,
    layer,
    psc_peaktime_ms,
    reversal_potential_at_peaktime_mv,
    increased_firing,
    decreased_firing,
    further_comments)
VALUES (%s,
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

cur.execute('SELECT * FROM postsynaptic_currents_and_firing')
description = cur.description

for column_number in range(len(description)):
    column_name = description[column_number][0]
    if (column_name != 'date_of_recording') and (
            column_name != 'neuron_number') and (
            column_name != 'slice_number'):
        update_query = '''
        UPDATE postsynaptic_currents_and_firing
        SET {current_column1} = NULL
        WHERE {current_column2} = 'NaN'
        '''.format(current_column1=column_name, current_column2=column_name)
        cur.execute(update_query)

conn.commit()

# %% add the column firing_modulation

cur.execute('''
    ALTER TABLE postsynaptic_currents_and_firing ADD COLUMN firing_modulation
    VARCHAR(19)
    ''')

conn.commit()

# %% add the values in the column firing_modulation

update_query = '''UPDATE postsynaptic_currents_and_firing
SET firing_modulation = 'increase'
WHERE increased_firing = 1 AND
decreased_firing  = 0
'''
cur.execute(update_query)

update_query = '''UPDATE postsynaptic_currents_and_firing
SET firing_modulation = 'decrease'
WHERE increased_firing = 0 AND
decreased_firing  = 1
'''
cur.execute(update_query)

update_query = '''UPDATE postsynaptic_currents_and_firing
SET firing_modulation = 'no_change'
WHERE increased_firing = 0 AND
decreased_firing  = 0
'''
cur.execute(update_query)

update_query = '''UPDATE postsynaptic_currents_and_firing
SET firing_modulation = 'increase_&_decrease'
WHERE increased_firing = 1 AND
decreased_firing  = 1
'''
cur.execute(update_query)

conn.commit()

# %% close the connection

conn.close()
