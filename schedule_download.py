#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 26 13:02:28 2020

@author: trismegistos

function:  scheduled task for Heroku Scheduler every day 4:30 to retrieve  updated data.
 Download data from list of git repos, transforms them in df and
 loads it to Heroku Postgres db
"""
import requests
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import extras #definitive import neccessary 
import os

def infect(source):
    '''
    Prepare base dataframe from csv. 
     - removes Province
     - removes geo data
     - corrects names
     - Add Wold count, 
     - add null selection
     - add daily increas rolling count
     - set index date
    Returns timeindex and cols of countries saved as .csv
    '''
    df = pd.read_csv(source)
    df.loc[df['Province/State'] == 'Hong Kong', 'Country/Region'] = 'Hong Kong*'
    df.loc[df['Province/State'] == 'Macau', 'Country/Region'] = 'Macau*'
    df.drop(['Lat','Long','Province/State'], axis = 1, inplace=True)
    
    # add World count
    df = df.append( pd.Series(['World'], index = ['Country/Region']).append(df.sum()[1:]), ignore_index =True)
    
    df = df.groupby('Country/Region').sum()   
    df = df.T
    
    #add Null selection
    df['No selection'] = np.NaN
    
    #calculate daily increase
    for i in df.columns:
        df[i+'_daily'] = df[[i]].diff(axis = 0, periods = 1)
    
    df.columns.name = 'Country'
    df.index.name = 'Date'

    return df


def pgres_load(df):
    """
    Saving df to Heroku Postgres
        - prepare data parts
        - delete previous table
        - create new skeleton table
        - populate data
    Table need to be initialzed through Heroku psql the very first time of deployment !
    """

    df_columns = list(df) 

    create_cols ='"Date" text,' + ",".join(['"'+ i+'"'+' float8' for i in df_columns[1:]])   
    columns = ",".join(['"'+ i+'"' for i in df_columns])
    values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 
    tbl_name = 'country'
    insert_stmt = "INSERT INTO {} ({}) {}".format(tbl_name,columns,values)
    
    
    #with engine.raw_connection() as conn: 
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        cur = conn.cursor()
        #clear up previous data and make a skeleton table
        try: # if table not exists
            cur.execute('DROP TABLE '+ tbl_name +';')
        except : 
            # if try doesnt execute connection needs to be cut
            cur.connection.rollback()
        
        cur.execute('CREATE TABLE '+ tbl_name +'({});'.format(create_cols))
        conn.commit()
        
        #fill up the skeletn table
        psycopg2.extras.execute_batch(cur, insert_stmt, df.values)
        conn.commit()
    return print('data saved')

confirmed_global = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
dead_global = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'
DATABASE_URL = os.environ['DATABASE_URL']

#list of sources
pull = [confirmed_global,dead_global] #source
save = ['global_confirmed.csv','global_dead.csv'] #temporal csv filenames

for p,s in zip(pull,save):
    r = requests.get(p).text
    with open(s, 'w') as f:
        f.write(r)

sick = infect('global_confirmed.csv')
dead = infect('global_dead.csv')

dead.columns = [i+'_dead' for i in dead.columns]
final = pd.concat((sick,dead), axis = 1) 
final.reset_index(inplace = True)

pgres_load(final)
