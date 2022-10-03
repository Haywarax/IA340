#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas
import configparser
import psycopg2


# In[2]:


config = configparser.ConfigParser()
config.read('config.ini')

host=config['myaws']['host']
db=config['myaws']['db']
user=config['myaws']['user']
pwd=config['myaws']['pwd']


# In[3]:


conn = psycopg2.connect(host=host,
                       user=user,
                       password=pwd,
                       dbname=db)


# In[4]:


sql_statement ="""
                select *
                from student"""


# In[5]:


df=pandas.read_sql_query(sql_statement,conn)

df[:]


# In[6]:


sql ="""
                select professor.p_name, course.c_name
                from professor
                inner join course
                on professor.p_email = course.p_email"""


# In[7]:


df=pandas.read_sql_query(sql,conn)

df [:]


# In[8]:


sql ="""
                select c_number, count (c_number) as number_of_students
                from enroll
                group by c_number"""


# In[9]:


df=pandas.read_sql_query(sql,conn)

df [:]


# In[10]:


df = pandas.read_sql_query(sql,conn)

df.plot.bar(y='number_of_students',x='c_number')


# In[11]:


sql ="""
                select professor.p_name, count(course.c_name) as num_courses
                from professor
                inner join course
                on professor.p_email = course.p_email
                group by professor.p_name
                order by num_courses desc"""


# In[12]:


df=pandas.read_sql_query(sql,conn)

df [:]


# In[13]:


df = pandas.read_sql_query(sql,conn)

df.plot.bar(x='p_name',y='num_courses')


# In[14]:


sql ="""select * from professor"""


# In[15]:


df=pandas.read_sql_query(sql,conn)

df [:]


# In[16]:


sql ="""insert into professor (p_email,p_name,office)
        values ('{}','{}','{}')""".format('p4@jmu.edu','p4','o4')

print(sql)


# In[17]:


cur=conn.cursor()


# In[18]:


cur.execute(sql)


# In[19]:


sql ="""select * from professor"""


# In[20]:


df=pandas.read_sql_query(sql,conn)

df [:]


# In[21]:


sql ="""select * from course"""


# In[22]:


df=pandas.read_sql_query(sql,conn)

df [:]


# In[23]:


sql ="""insert into course (c_number,c_name,room,p_email)
        values ('{}','{}','{}','{}')""".format('c5','linkedin','r4','p4@jmu.edu')

print(sql)


# In[24]:


cur=conn.cursor()


# In[25]:


cur.execute(sql)


# In[26]:


sql ="""select * from course"""


# In[27]:


df=pandas.read_sql_query(sql,conn)

df [:]


# In[31]:


sql ="""update course 
        set p_email = 'p4@jmu.edu'
        where p_email = 'p2@jmu.edu'"""

print(sql)


# In[32]:


cur=conn.cursor()


# In[33]:


cur.execute(sql)


# In[34]:


sql ="""select * from course"""


# In[35]:


df=pandas.read_sql_query(sql,conn)

df [:]


# In[36]:


sql ="""delete from professor 
        where p_email = 'p2@jmu.edu'"""

print(sql)


# In[37]:


cur=conn.cursor()


# In[38]:


cur.execute(sql)


# In[39]:


sql ="""select * from professor"""


# In[40]:


df=pandas.read_sql_query(sql,conn)

df [:]


# In[ ]:




