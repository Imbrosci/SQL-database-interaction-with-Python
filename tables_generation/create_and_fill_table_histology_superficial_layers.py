# -*- coding: utf-8 -*-
"""
Created on Mon Dec 13 12:07:09 2021

Create and fill a table with the histology and immunostaining information from
biocytin-filled patched-clamp neurons from the entorhinal cortex superficial
layers.

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

create_table_histology_superficial_layers = '''
CREATE TABLE histology_superficial_layers(
    date_of_recording DATE,
    neuron_number SMALLINT,
    slice_number SMALLINT,
    superficial_layer NUMERIC(1,0),
    spines VARCHAR(3),
    calbindin_staining VARCHAR(8),
    reelin_staining VARCHAR(8),
    morphology VARCHAR(50),
    comments_on_position TEXT,
    comments_on_staining TEXT,
    PRIMARY KEY(Date_of_recording, Neuron_number)
    );
'''

cur.execute(create_table_histology_superficial_layers)
conn.commit()

# %% load the file with the values and organize the values as a list of tuples

filepath = 'path/to/file'

df = pd.read_excel(filepath, sheet_name='Histology_immuno', header=0)
df = df.sort_values(by='Date_of_recording')

values = []
for c, r in df.iterrows():
    row = []
    for element in r:
        row.append(element)
    values.append(tuple(row))

# %% insert the values into the table

insert_query = ''' INSERT INTO histology_superficial_layers(
    date_of_recording,
    neuron_number,
    slice_number,
    superficial_layer,
    spines,
    calbindin_staining,
    reelin_staining,
    morphology,
    comments_on_position,
    comments_on_staining)
VALUES (%s,
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

cur.execute('SELECT * FROM histology_superficial_layers')
description = cur.description

for column_number in range(len(description)):
    column_name = description[column_number][0]
    if (column_name != 'date_of_recording') and (
            column_name != 'neuron_number') and (
            column_name != 'slice_number'):
        update_query = '''
        UPDATE histology_superficial_layers
        SET {current_column1} = NULL
        WHERE {current_column2} = 'NaN'
        '''.format(current_column1=column_name, current_column2=column_name)
        cur.execute(update_query)

conn.commit()

# %% close the connection

conn.close()
