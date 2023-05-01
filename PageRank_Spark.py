#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.format("csv").option("header", "false").option("sep","\t").option("inferSchema", "true").load("gs://csee4121hw2/task2_small.csv").dropna()


# In[2]:


rank = df.select(["_c0","_c1"]).rdd.flatMap(lambda x : [x[0],x[1]]).distinct().map(lambda x: (x,1))
rank_df = rank.toDF().withColumnRenamed("_1","rank_link").withColumnRenamed("_2","rank_value")
link = df.select(["_c1"]).rdd.flatMap(lambda x : x).distinct().map(lambda x: (x,1)) # link

only_title = rank.subtractByKey(link).distinct().map(lambda x: (x[0],0)) # in title not in link

count_title = df.rdd.map(lambda x: (x[0],1)).reduceByKey(lambda y,z: y+z).toDF().withColumnRenamed("_1","count_link").withColumnRenamed("_2","count_value") # title's number of neighbours    

joined_table = df.join(count_title,df["_c0"] == count_title["count_link"]).select(["_c0", "_c1","count_value"])



# In[3]:


def linkMatching(count_title,joined_table, rank, only_title): 
    
    whole_table = joined_table.join(rank,joined_table["_c0"] == rank["rank_link"]).select(["_c0", "_c1", "count_value","rank_value"])
    
    rank_table = whole_table.withColumn("contribution", whole_table.rank_value/whole_table.count_value)
    
    new_rdd = rank_table.select(["_c1","contribution"]).rdd.reduceByKey(lambda x,y: x+y).map(lambda y: (y[0],y[1]*0.85+0.15))
    
    new_table = new_rdd.union(only_title).toDF().fillna(0).withColumnRenamed("_1","rank_link").withColumnRenamed("_2","rank_value")
    
    return new_table


# In[4]:

rank_new = linkMatching(count_title,joined_table,rank_df, only_title)

for i in range(9):
    rank_new = linkMatching(count_title,joined_table,rank_new, only_title)


# In[5]:


sorted_rank = rank_new.sort(["rank_link","rank_value"],ascending=True)


# In[6]:


# sorted_rank.show()


# In[7]:


gcs_bucket = 'csee4121hw2' 
gcs_filepath = 'gs://{}/p1t3.csv'.format(gcs_bucket)


# In[8]:


sorted_rank.filter("rank_value > 0").limit(5).write.csv(path=gcs_filepath, header="true", mode="overwrite", sep="\t")


# In[ ]:




