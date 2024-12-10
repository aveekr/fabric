# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "92528399-4196-49ee-b61b-1b2dbac4f9f7",
# META       "default_lakehouse_name": "SDS_ESGDE_sustain_2_ComputedESGMetrics_LH",
# META       "default_lakehouse_workspace_id": "33b35682-feda-47b7-967b-3905434352d7",
# META       "known_lakehouses": [
# META         {
# META           "id": "92528399-4196-49ee-b61b-1b2dbac4f9f7"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Overview
# This notebook aggregates emissions data in the ‘IngestedRawData’ Lakehouse and stores it in ‘AggregatedData’ Lakehouse. This creates new aggregated tables which will be compatible for the report.
# 
# For more information and prerequisite [click here](https://go.microsoft.com/fwlink/?linkid=2288426) to view Azure Emissions Data Estate documentation

# MARKDOWN ********************

# ### Format columns in the existing table

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *

df = spark.sql("SELECT * FROM azure_emissions")
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
df = df.withColumn("DateFormat",to_date("DateKey","yyyyMMdd"))
df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save("Tables/azure_emissions")

# METADATA ********************

# META {}

# MARKDOWN ********************

# ### Create new emissions summary table to generate aggregated data

# MARKDOWN ********************

# Create a new table for report

# CELL ********************

# MAGIC %%sql
# MAGIC create table emissions_summary (TotalEmissions varchar(100), DateFormat date, SubscriptionId varchar(100), SubscriptionName varchar(100), ResourceId varchar(1800), ResourceName varchar(1024), AzureRegionDisplayName varchar(100), ServiceCategory varchar(100), BillingId varchar(100))

# METADATA ********************

# META {
# META   "language": "sparksql"
# META }

# MARKDOWN ********************

# Insert data into the new table

# CELL ********************

# MAGIC %%sql
# MAGIC insert into emissions_summary select sum(Scope1CarbonEmission + Scope2MarketCarbonEmission) + sum(Scope3CarbonEmission) as TotalEmissions, DateFormat, SubscriptionId, SubscriptionName, ResourceName as ResourceId, ResourceName, AzureRegionDisplayName, ServiceCategory, EnrollmentNumber as BillingId from azure_emissions group by DateFormat, SubscriptionId, SubscriptionName, ResourceId, ResourceName, AzureRegionDisplayName, ServiceCategory, BillingId

# METADATA ********************

# META {
# META   "language": "sparksql"
# META }

# MARKDOWN ********************

# Format the columns of the new table

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *

df1 = spark.sql("SELECT * FROM emissions_summary")

df1= df1.withColumn("MonthYear",date_format("DateFormat","MMM-yyyy"))
df1.write.format("delta").mode("overwrite").option("overwriteSchema", True).save("Tables/emissions_summary")

# MARKDOWN ********************

# View data

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM emissions_summary LIMIT 100


# METADATA ********************

# META {
# META   "language": "sparksql"
# META }

# MARKDOWN ********************

# ### Generate aggregated data

# MARKDOWN ********************

# Emissions by region

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT SUM(TotalEmissions) as Emissions, AzureRegionDisplayName from emissions_summary GROUP BY AzureRegionDisplayName


# METADATA ********************

# META {
# META   "language": "sparksql"
# META }

# MARKDOWN ********************

# Emissions by subscription

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT SUM(TotalEmissions) as Emissions, SubscriptionName from emissions_summary GROUP BY SubscriptionName

# METADATA ********************

# META {
# META   "language": "sparksql"
# META }

# MARKDOWN ********************

# Emissions by resource

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT SUM(TotalEmissions) as Emissions, ResourceName from emissions_summary GROUP BY ResourceName

# METADATA ********************

# META {
# META   "language": "sparksql"
# META }

# MARKDOWN ********************

# Total emissions

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT SUM(TotalEmissions) as Emissions from emissions_summary

# METADATA ********************

# META {
# META   "language": "sparksql"
# META }
