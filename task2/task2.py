#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
import re
import xml.etree.ElementTree as ET
from pyspark.sql.functions import udf, explode, col, when, lower
from pyspark.sql.types import ArrayType, StringType
spark = SparkSession.builder.getOrCreate()
df = spark.read.format('xml').options(rowTag='page').load('hdfs:/wiki-small.xml')
#df.printSchema()


# In[2]:


import re
def extract_text(text):
    try:
        match = re.findall(r'\[\[[^\[\]]+\]\]',text)
    except:
        match = []
    output = []
    for link in match:
        temp = link.split('|')[0].lower()
        if (':' in temp) and ('Category:' not in temp):
            continue
        elif '#' in temp:
            continue
        else: output.append(temp)
    return [re.sub(r'\[|\]','',a) for a in output]


# In[3]:


udf_extract_text = udf(lambda x:extract_text(x), ArrayType(StringType()))
df2 = df.select(col('title'), col('revision.text._VALUE').alias('links'))
df2 = df2.withColumn('internal_links',explode(udf_extract_text(df2['links'])))
df2 = df2.select(lower(col('title')).alias('title'),col('internal_links').alias('internal_links'))
df2 = df2.select(col('title'),col('internal_links')).na.drop()
df2  = df2.select(col('title'),col('internal_links')).sort(['title','internal_links'],ascending=True)


# In[5]:


df2.limit(10).write.csv('gs://programming-hw2-1-bucket/notebooks/jupyter/task2', mode = 'overwrite',sep='\t')
#df2.coalesce(1).write.option('delimeter', '\t').csv('gs://programming-hw2-1-bucket/notebooks/jupyter/task2_whole', mode = 'overwrite')


# In[ ]:




