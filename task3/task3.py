#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.14.0 pyspark-shell'


# In[2]:


#!/usr/bin/env python
# coding: utf-8

# In[1]:
from pyspark.sql import SparkSession
import re
import xml.etree.ElementTree as ET
from pyspark.sql.functions import udf, explode, col, when, lower
from pyspark.sql.types import ArrayType, StringType
from operator import add
spark = SparkSession.builder.getOrCreate()
df = spark.read.format('xml').options(rowTag='page').load('hdfs:/wiki-whole.xml')
#df.printSchema()


# In[3]:



# In[4]:


import re
#write the udf and regrex function
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


# In[5]:


# In[3]:

udf_extract_text = udf(lambda x:extract_text(x), ArrayType(StringType()))
#select columns that we need
df2 = df.select(col('title'), col('revision.text._VALUE').alias('links'))
#explode the links column
df2 = df2.withColumn('internal_links',explode(udf_extract_text(df2['links'])))
#lower all the rows
df2 = df2.select(lower(col('title')).alias('title'),col('internal_links').alias('internal_links'))
#drop na rows
df2 = df2.select(col('title'),col('internal_links')).na.drop()
#sort rows in ascending order
df2  = df2.select(col('title'),col('internal_links')).sort(['title','internal_links'],ascending=True)


# In[6]:




# In[7]:




# In[8]:


articles = df2.select(col('title')).distinct().union(df2.select(col('internal_links')).distinct())


# In[9]:



# In[10]:


ranks = articles.rdd.map(lambda url_neighbors: (url_neighbors[0], 1.0))


# In[11]:


iterations = 10


# In[12]:


df2_rdd = df2.rdd.groupByKey().cache()


# In[13]:




# In[14]:



# In[15]:




# In[16]:


def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


# In[17]:


from operator import add
for iteration in range(iterations):
    contribution = df2_rdd.join(ranks).flatMap(lambda x: computeContribs(
            x[1][0], x[1][1]  # type: ignore[arg-type]
        ))
    # (article0, (neighbours, rank)) -> (article, contribution)
    ranks = contribution.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)


# In[ ]:


ranks_df= ranks.toDF().withColumnRenamed("_1","article").withColumnRenamed("_2","rank").sort(["rank"], ascending=False)


# In[ ]:




# In[ ]:


ranks_df.limit(10).toPandas().to_csv('work/task3.csv',header = False, index = False, sep='\t')


# In[ ]:




