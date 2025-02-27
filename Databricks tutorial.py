# Databricks notebook source
# MAGIC %md
# MAGIC ## Connect
# MAGIC

# COMMAND ----------

# from pyspark.sql import SparkSession

# SparkSession.builder.master("local[*]").getOrCreate().stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Reading

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema', True).option('header', True).load('/FileStore/tables/BigMart_Sales-1.csv')

# COMMAND ----------

df

# COMMAND ----------

df.head()

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show(1)

# COMMAND ----------

df.show(1, vertical=True)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DDL Schema

# COMMAND ----------


my_ddl_schema = '''
                    Item_Identifier STRING,
                    Item_Weight STRING,
                    Item_Fat_Content STRING, 
                    Item_Visibility DOUBLE,
                    Item_Type STRING,
                    Item_MRP DOUBLE,
                    Outlet_Identifier STRING,
                    Outlet_Establishment_Year INT,
                    Outlet_Size STRING,
                    Outlet_Location_Type STRING, 
                    Outlet_Type STRING,
                    Item_Outlet_Sales DOUBLE 

                ''' 

# COMMAND ----------

df = spark.read.format('csv')\
            .schema(my_ddl_schema)\
            .option('header',True)\
            .load('/FileStore/tables/BigMart_Sales.csv') 

# COMMAND ----------

# MAGIC %md
# MAGIC ## TRANSFORMATIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ### SELECT

# COMMAND ----------

from pyspark.sql.types import * 
from pyspark.sql.functions import *  
     

# COMMAND ----------

df.select('Item_Identifier', 'Item_Weight', 'Item_Visibility').display()

# COMMAND ----------

df.select(col('Item_Identifier'), col('Item_Weight'), col('Item_Visibility')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ALIAS

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### FILTER

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 1:

# COMMAND ----------

df.filter(col('Item_Fat_Content') == 'Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 2:
# MAGIC

# COMMAND ----------

df.filter((col('Item_Type') == 'Soft Drinks') & (col('Item_Weight')<10)).display()  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 3:

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 2'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column Renamed

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_Wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 1

# COMMAND ----------

df = df.withColumn('flag', lit("new"))

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('multiply', col('Item_Weight')*col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 2:

# COMMAND ----------

df.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), 'Regular', 'Reg'))\
    .withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), 'Low Fat', 'Lf'))\
    .display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Typecasting

# COMMAND ----------

df.withColumn('Item_Weight', col('Item_Weight').cast(StringType())).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Sort/Order By

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Scenario 3

# COMMAND ----------

df.sort(['Item_Weight', 'Item_Visibility'], ascending=[0,0]).display()

# COMMAND ----------

df.sort(['Item_Weight', 'Item_Visibility'], ascending=[1,0]).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### LIMIT

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROP

# COMMAND ----------

df.drop(col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop Duplicates

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df.dropDuplicates(subset= ['Item_Type']).display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### UNION

# COMMAND ----------


