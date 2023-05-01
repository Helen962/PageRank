#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.format('xml').options(rowTag='page').load('hdfs:/enwiki_small.xml')


# In[2]:


from pyspark.sql.types import StringType,ArrayType
from pyspark.sql.functions import udf,col,lower
from pyspark.sql.functions import explode
import regex
import re


# In[3]:


def linkMatching(line):
    ans = []
    result = str(line).split("text=Row(_VALUE=")[1]
    res = regex.findall('\[\[((?:[^[\]]+|(?R))*+)\]\]', str(result).lower())
    for i in res:
        if "|" in i:
            a = i.split("|")
            idx = 0
            while(idx < len(a)):
                if (':' in a[idx]) and (not 'category' in a[idx]):
                    idx += 1
                elif '#' in i:
                    idx += 1
                else:
                    break
            if idx < len(a):
                ans.append(a[idx])
                    
        else:
            if (':' in i) and (not 'category' in i):
                pass
            elif '#' in i:
                pass
            else:
                ans.append(i)
    return ans


# In[4]:


new_df = df.withColumn('title', lower(df.title))

linkMatchingUdf = udf(lambda line: linkMatching(line), ArrayType(StringType(), False))
articleLines = new_df.withColumn( 'articleMatch', linkMatchingUdf(col("revision")))

matchdf = articleLines.select(["title", explode("articleMatch").alias("link")])

sorted_df = matchdf.sort(['title','link'], ascending=True)


# In[7]:


gcs_bucket = 'csee4121hw2'  
gcs_filepath_new = 'gs://{}/alpha/p1t2.csv'.format(gcs_bucket)


# In[8]:


sorted_df.limit(5).write.csv(path=gcs_filepath_new, header="false", mode="overwrite", sep="\t")


# In[ ]:




