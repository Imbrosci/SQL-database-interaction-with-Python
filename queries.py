# -*- coding: utf-8 -*-
"""
Created on Wed Dec 15 09:54:20 2021

the code containes useful queries and it offers an example on how to execute a
query and to get the output in a pandas DataFrame.
It also shows an example on how to save (export as an excel file) the results
and to plot some output data.

@author: imbroscb
"""

import psycopg2 as pg2
import pandas as pd
import seaborn

# %% Frequently used queries

# get the mean and standard deviation of the PSPs amplitude coupled to
# standard SWRs for neurons in the deep and superficial layers of the
# entorhinal_cortex
query_1 = '''SELECT layer,
ROUND(AVG(psps_coupled_to_standard_amplitude_mv), 3) AS mean_psp_amplitude,
ROUND(STDDEV(psps_coupled_to_standard_amplitude_mv), 3) AS std_psp_amplitude
FROM postsynaptic_potentials
GROUP BY layer'''

# get the mean and standard deviation of the input resistance for neurons in
# the deep and superficial layers of the entorhinal cortex
query_2 = '''SELECT layer, ROUND(AVG(input_resistance_mohm), 3) AS mean_Ri,
ROUND(STDDEV(input_resistance_mohm), 3) AS std_Ri
 FROM intrinsic_properties
GROUP BY layer'''

# get the mean and standard deviation of the reversal potential at PSCs
# negative peaktime for neurons in the deep and superficial layers of the
# entorhinal cortex
query_3 = '''SELECT layer,
ROUND(AVG(reversal_potential_at_peaktime_mv),3) AS mean_rev_pot,
ROUND(STDDEV(reversal_potential_at_peaktime_mv),3) AS std_rev_pot
FROM postsynaptic_currents_and_firing
GROUP BY (layer)'''

# get the mean and standard deviation of the reversal potential at PSCs
# negative peaktime for neurons in the deep and superficial layers of the
# entorhinal_cortex and also grouped by the firing modulation
query_4 = '''SELECT firing_modulation, layer,
ROUND(AVG(reversal_potential_at_peaktime_mv),3) AS mean_rev_pot,
ROUND(STDDEV(reversal_potential_at_peaktime_mv),3) AS std_rev_pot
FROM postsynaptic_currents_and_firing
WHERE firing_modulation NOTNULL
GROUP BY (layer, firing_modulation)'''

# get the mean and standard deviation of the PSCs peaktime for neurons in the
# deep and superficial layers of the entorhinal_cortex
query_5 = '''SELECT layer, ROUND(AVG(psc_peaktime_ms),3) AS mean_peaktime,
ROUND(STDDEV(psc_peaktime_ms),3) AS std_peaktime
FROM postsynaptic_currents_and_firing
WHERE psc_peaktime_ms NOTNULL
GROUP BY (layer)'''

# get the mean reversal potential at PSCs negative peaktime and the mean PSPs
# amplitude for neurons grouped by their firing modulation
query_6 = '''SELECT firing_modulation,
ROUND(AVG(reversal_potential_at_peaktime_mv),3) AS mean_rev_pot,
ROUND(AVG(psps_coupled_to_standard_amplitude_mv),3) AS mean_psp_amp
FROM postsynaptic_currents_and_firing
INNER JOIN postsynaptic_potentials
ON postsynaptic_currents_and_firing.date_of_recording =
postsynaptic_potentials.date_of_recording AND
postsynaptic_currents_and_firing.neuron_number =
postsynaptic_potentials.neuron_number
WHERE firing_modulation NOTNULL
GROUP BY (postsynaptic_currents_and_firing.firing_modulation)'''

# bring together information on PSPs amplitude,firing_modulation and reversal
# potential (from two tables), in only two neurons the firing increased and
# decreased (or the other way around), these two neurons are not considered
query_7 = '''SELECT postsynaptic_currents_and_firing.date_of_recording,
postsynaptic_currents_and_firing.neuron_number,
postsynaptic_currents_and_firing.layer, firing_modulation,
reversal_potential_at_peaktime_mv,
psps_coupled_to_standard_amplitude_mv FROM postsynaptic_currents_and_firing
INNER JOIN postsynaptic_potentials
ON postsynaptic_currents_and_firing.date_of_recording =
postsynaptic_potentials.date_of_recording AND
postsynaptic_currents_and_firing.neuron_number =
postsynaptic_potentials.neuron_number
WHERE firing_modulation NOTNULL AND firing_modulation != 'increase_&_decrease'
ORDER BY postsynaptic_currents_and_firing.date_of_recording'''

# get the mean and standard deviation of the reversal potential for neurons in
# the supercial layers of the entorhinal cortex groupped accordingly to their
# location in layer 2 or layer 3
query_8 = '''SELECT superficial_layer,
ROUND(AVG(reversal_potential_at_peaktime_mv),3) AS mean_revpot,
ROUND(STDDEV(reversal_potential_at_peaktime_mv),3) AS std_revpot
FROM histology_superficial_layers
INNER JOIN postsynaptic_currents_and_firing
ON histology_superficial_layers.date_of_recording =
postsynaptic_currents_and_firing.date_of_recording
AND histology_superficial_layers.neuron_number =
postsynaptic_currents_and_firing.neuron_number
WHERE superficial_layer NOTNULL
GROUP BY superficial_layer'''

# %% connect with PostgreSQL

# create a connection with PostgreSQL
conn = pg2.connect(database='SWRs_in_entorhinal_cortex', user='your_username',
                   password='your_password')

# create a cursor object
cur = conn.cursor()

# %% example one

# get the table with the output of the query
cur.execute(query_1)

# get the columns
description = cur.description
columns = []
for column_number in range(len(description)):
    columns.append(description[column_number][0])

# get the rows
values = cur.fetchall()

# fill the dataframe
df = pd.DataFrame(columns=columns)
index = 0
for row in values:
    counter = 0
    for column in columns:
        df.loc[index, column] = row[counter]
        counter += 1
    index += 1

# %% example 2, generate and export a new joined table and plot some results

# get the firing categories
cur.execute('''SELECT DISTINCT(firing_modulation)
            FROM postsynaptic_currents_and_firing
                WHERE firing_modulation NOTNULL AND
                firing_modulation != 'increase_&_decrease'
                ''')

firing_categories_tuples = cur.fetchall()
firing_categories = []
for i in range(len(firing_categories_tuples)):
    firing_categories.append(firing_categories_tuples[i][0])

del firing_categories_tuples

# get the table with the data
cur.execute(query_7)

# get the columns
description = cur.description
columns = []
for column_number in range(len(description)):
    columns.append(description[column_number][0])

# get the rows
values = cur.fetchall()

# fill the dataframe
df = pd.DataFrame(columns=columns)
index = 0
for row in values:
    counter = 0
    for column in columns:
        df.loc[index, column] = row[counter]
        counter += 1
    index += 1

df.to_excel('path/to/file')
# plot some results
seaborn.relplot(data=df, x=columns[4], y=columns[5], hue=columns[3],
                hue_order=firing_categories)
