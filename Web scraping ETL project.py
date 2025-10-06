#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#The aim of this project is to perform an ETL process: Extract data from a website (Worldometer),Transform it in Python and Load
# it into Postgresql database


# In[1]:


#Step 1: Import your libraries
import pandas as pd


# ##### Extract data from website- Worldometer

# In[2]:


url = "https://www.worldometers.info/world-population/nigeria-population/"


# In[6]:


table1 = pd.read_html(url)[0]
table1.head()


# In[7]:


table2 = pd.read_html(url)[1]
table2.head()


# In[8]:


table3 = pd.read_html(url)[2]
table3.head()


# #### Transforming the data

# In[11]:


#Transform the headers - Remove the spaces between the column headers.
table1.columns = ['Year', 'Population', 'Yearly%Change', 'YearlyChange',
       'Migrants(net)', 'MedianAge', 'FertilityRate', 'Density(P/Km²)',
       'UrbanPop%', 'UrbanPopulation', 'CountrysShareofWorldPop',
       'WorldPopulation', 'NigeriaGlobalRank']

table1.head()


# In[13]:


table2.columns = ['Year', 'Population', 'Yearly%Change', 'YearlyChange',
       'Migrants(net)', 'MedianAge', 'FertilityRate', 'Density(P/Km²)',
       'UrbanPop%', 'UrbanPopulation', 'CountrysShareofWorldPop',
       'WorldPopulation', 'NigeriaGlobalRank']

table2.head()


# In[15]:


table3.columns = ['SerialNum', 'City', 'Population']

table3.head()


# #### Load data into PostgreSQL Database

# In[17]:


#Import the necessary libraries
#psycopg2 For connecting to postgresql database and executing queries
#sqlalchemy for efficiently managing database connections and reuse of connections.

import psycopg2 
import sqlalchemy  


# In[18]:


from sqlalchemy import create_engine


# In[19]:


#Database Credentials
db_username = 'postgres'
db_password = 'mekoma'
db_host = 'localhost'
db_port = 5432
db_name = 'nigeria_population'


# In[20]:


#Establish connection to the database with sqlalchemy engine

engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')


# In[21]:


dbtb1 = 'pop2025'
dbtb2 = 'popforecast'
dbtb3 = 'popcity'

# Load data into database tables
table1.to_sql(dbtb1, engine, if_exists= 'replace', index=False)
table2.to_sql(dbtb2, engine, if_exists= 'replace', index=False)
table3.to_sql(dbtb3, engine, if_exists= 'replace', index=False)

# close connection
engine.dispose()


# In[ ]:




