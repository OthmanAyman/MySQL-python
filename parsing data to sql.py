#!/usr/bin/env python
# coding: utf-8

# Written by: Othman Ayman

# ## connect to mySQL database:

# In[66]:


import mysql.connector as connection

try:
    
    mydb = connection.connect(host="localhost", database='test', user="root", passwd="*****",use_pure=True)
    cur = mydb.cursor()

except Exception as e:
    mydb.close()
    print(str(e))


# ## read data from csv file:
# source of the csv dataset: https://archive.ics.uci.edu/ml/machine-learning-databases/00448/

# In[67]:


import csv


# ## clean data and parsing to SQL database: 

# In[68]:


with open('carbon_nanotubes.csv','r') as f:
    data = csv.reader(f, delimiter=';')
    for row in enumerate(data):
        # extract columns
        if row[0] == 0 : 
            columns = row[1]
            
            print(f"columns name:\n{columns}\n\n")    
            
            # extract query string for creating table
            query =""
            for column in columns:
                column = column.replace(" ",'_')
                column = column.replace("\'",'11')
                query += column +" FLOAT(20),"
            query = query[0:-1]  
            print(query+'\n\n')
            
            #creating the table 
            cur.execute(f'CREATE TABLE carbon_nanotubes({query})')

        else:
            # extract values from data
            values = row[1]
            

            # set query string to insert data
            query =""
            for value in values:
                value = float(value.replace(',','.'))
                query += str(value) +","
            query = query[0:-1]  
#             print(query)

            # insert data to table:
            cur.execute(f"insert into carbon_nanotubes value({query})")

# commit writing to database
mydb.commit()   
print("Done parsing data successfully!")


# ## fetch data we inserted to check:

# In[69]:


if mydb.is_connected:
    cur.execute('SELECT * FROM carbon_nanotubes')
    result = cur.fetchall()

    
for row in result:
    print (row)


# ## close connection 

# In[70]:



# terminate connection 
mydb.close()


# ### Drop the table
# in case you want to run again!

# In[71]:


mydb = connection.connect(host="localhost", database='test', user="root", passwd="****",use_pure=True)
cur = mydb.cursor()
cur.execute('drop table carbon_nanotubes')
mydb.commit()

