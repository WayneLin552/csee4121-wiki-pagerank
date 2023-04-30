#!/usr/bin/env python
# coding: utf-8

# In[9]:


from pyspark.sql import SparkSession
import pandas as pd
from collections import Counter


# In[10]:


spark = SparkSession.builder.getOrCreate()
df = spark.read.csv('gs://programming-hw2-1-bucket/notebooks/jupyter/task2/part-00000-4c276f20-f2fd-4803-8321-6ce555355788-c000.csv',
                   inferSchema=True,header=False,sep='\t')


# In[30]:


#df.show()


# In[16]:


def get_article_counter(input_file):
    '''
    get articles and their related titles, then get counter of neighbors
    '''
    articles={}
    counter = []
    for item in input_file.collect():
        left, right = item
        counter.append(left)
        articles[right] = articles.get(right,[])+[left]
        articles[left] = articles.get(left,[])
    counter = Counter(counter)
    return articles, counter

def init_rank(articles):
    '''
    initial ranks
    '''
    ranks = {}
    for article in articles:
        ranks[article] = 1
    return ranks

def cal_contributions(articles,counter,ranks):
    '''
    calculate the contributions of each article
    '''
    contributions = {}
    for article in articles:
        contributions[article]=0
        neighbor = articles[article]
        for i in neighbor:
            contributions[article] +=ranks[i]/counter[i]
    return contributions

def update_rank(ranks, contributions):
    '''
    update ranks base on contributions
    '''
    for article in ranks:
        ranks[article] = 0.15+0.85*contributions[article]
    return ranks
        


# In[28]:


if __name__=='__main__':
    articles, counter = get_article_counter(df)
    ranks = init_rank(articles)
    for i in range(10):
        contributions = cal_contributions(articles,counter,ranks)
        ranks = update_rank(ranks,contributions)
    result = pd.DataFrame(ranks.items(),columns=['title','rank'])
    result = result.sort_values(by=['rank'],ascending=False)
    result.head(10).to_csv('gs://programming-hw2-1-bucket/notebooks/jupyter/task3.csv',header=False,sep='\t',index=False,encoding='utf-8')
        


# In[29]:


#result.sort_values(by=['rank','title'],ascending=True).head()


# In[ ]:





# In[ ]:




