-- Databricks notebook source
-- MAGIC %sql
-- MAGIC show tables

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from loan_analysiss 
-- MAGIC

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC describe loan_analysiss

-- COMMAND ----------

-- DBTITLE 1,Gender type loan distribution 
select gender , sum(loan) from loan_analysiss
group by Gender

-- COMMAND ----------

-- DBTITLE 1,Loan by occupation 
select distinct Occupation from loan_analysiss

-- COMMAND ----------

select Occupation, sum(Loan) from loan_analysiss
group by Occupation 
order by 2 desc
limit 15 

-- COMMAND ----------

-- DBTITLE 1,Loan Category Distribution 
select distinct 'Loan Category'  from loan_analysiss
limit 10

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyspark.sql.functions as F
-- MAGIC
-- MAGIC # Read the loan_analysiss table into a DataFrame
-- MAGIC loan_df = spark.read.table("loan_analysiss")
-- MAGIC
-- MAGIC # Group by Loan Category, calculate the sum of Loan, and order by descending sum
-- MAGIC grouped_df = loan_df.groupBy("Loan Category").agg(F.sum("Loan").alias("Total_Loan")).orderBy(F.desc("Total_Loan"))
-- MAGIC
-- MAGIC # Display all rows of the grouped DataFrame
-- MAGIC display(grouped_df)
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Loan by Marital Status 
-- MAGIC %python
-- MAGIC # Read the loan_analysiss table into a DataFrame
-- MAGIC loan_df = spark.read.table("loan_analysiss")
-- MAGIC
-- MAGIC # Group by Marital Status, calculate the sum of Loan, and order by Marital Status
-- MAGIC grouped_df = loan_df.groupBy("Marital Status").agg(F.sum("Loan")).orderBy("Marital Status")
-- MAGIC
-- MAGIC # Display all rows of the grouped DataFrame
-- MAGIC display(grouped_df)
