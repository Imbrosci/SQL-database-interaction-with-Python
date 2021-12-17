# -*- coding: utf-8 -*-
"""
Created on Fri Dec 17 11:50:14 2021

the code generates the SWRs_in_entorhinal cortex database

@author: imbroscb
"""

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2 as pg2

# %%

conn = pg2.connect(user='your_username', password='your_password')

conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cur = conn.cursor()

cur.execute('CREATE DATABASE SWRs_in_entorhinal_cortex')

conn.commit()
conn.close()
