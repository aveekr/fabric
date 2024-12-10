# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "77a5fe57-865d-44c5-9df9-f6095a39bde5",
# META       "default_lakehouse_name": "SDS_ESGDE_sustain_2_ConfigAndDemoData_LH",
# META       "default_lakehouse_workspace_id": "33b35682-feda-47b7-967b-3905434352d7",
# META       "known_lakehouses": [
# META         {
# META           "id": "77a5fe57-865d-44c5-9df9-f6095a39bde5"
# META         },
# META         {
# META           "id": "364efa17-f3fb-4077-b1e2-4efb25740cab"
# META         },
# META         {
# META           "id": "92528399-4196-49ee-b61b-1b2dbac4f9f7"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Overview
# This notebook is used for cleaning the existing tables and loading the data from the lake house to the tables.\
# The emissions data will be present in the Ingested Raw Data Lakehouse and the demo data will be present in the ‘ConfigAndDemoData’ Lakehouse.
# 
# For more information and prerequisite [click here](https://go.microsoft.com/fwlink/?linkid=2288426) to view Azure Emissions Data Estate documentation.

# MARKDOWN ********************

# ### Install required packages

# CELL ********************

! pip install sql-metadata

from notebookutils import mssparkutils

# Restart the Python session to apply the installed packages
mssparkutils.session.restartPython()

# MARKDOWN ********************

# ### Run utility notebook

# CELL ********************

%run "SDS_ESGDE_sustain_2_Utilities_INTB"

# MARKDOWN ********************

# ### Parameters

# MARKDOWN ********************

# Input parameters
# 
# __CONFIG_AND_DEMO_DATA_LAKEHOUSE_NAME__ : Lakehouse storing the Demodata\
# __RAW_DATA_EMISSSIONS_LAKEHOUSE_NAME__ : Lakehouse storing the azure emissions raw data\
# __TARGET_LAKEHOUSE_NAME__ : Name of the Lakehouse storing the aggregated data 

# CELL ********************

CONFIG_AND_DEMO_DATA_LAKEHOUSE_NAME = "SDS_ESGDE_sustain_2_ConfigAndDemoData_LH"
RAW_DATA_EMISSSIONS_LAKEHOUSE_NAME = "SDS_ESGDE_sustain_2_IngestedRawData_LH"
TARGET_LAKEHOUSE_NAME = "SDS_ESGDE_sustain_2_ComputedESGMetrics_LH"


# MARKDOWN ********************

# Derived parameters

# CELL ********************

config_and_demo_data_lakehouse_abfs_path = mssparkutils.lakehouse.get(CONFIG_AND_DEMO_DATA_LAKEHOUSE_NAME).get("properties").get("abfsPath")
raw_data_emissions_lakehouse_abfs_path = mssparkutils.lakehouse.get(RAW_DATA_EMISSSIONS_LAKEHOUSE_NAME).get("properties").get("abfsPath")

DEMO_DATA_ABFS_PATH = f"{config_and_demo_data_lakehouse_abfs_path}/Files/{Capability.MicrosoftAzureEmissionsInsights.value}/DemoData"
RAW_DATA_EMISSSIONS_ABFS_PATH = f"{raw_data_emissions_lakehouse_abfs_path}/Files/AzureEmissions"

# By default, we load the demo data into TargetLakehouse
# In order to load the actual emissions, kindly change the value of "SOURCE_ABFS_PATH" from "DEMO_DATA_ABFS_PATH" to "RAW_DATA_EMISSSIONS_ABFS_PATH" and run the notebook again.
SOURCE_ABFS_PATH = DEMO_DATA_ABFS_PATH

# MARKDOWN ********************

# ### Clean tables

# CELL ********************

spark.sql(f"DROP TABLE IF EXISTS {TARGET_LAKEHOUSE_NAME}.azure_emissions")
spark.sql(f"DROP TABLE IF EXISTS {TARGET_LAKEHOUSE_NAME}.emissions_summary")

# METADATA ********************

# META {}

# MARKDOWN ********************

# ### Initialize Configuration and Demo data for the capability

# CELL ********************

initialize_config_and_demo_data(Capability.MicrosoftAzureEmissionsInsights)

# MARKDOWN ********************

# ### Load tables to Fabric

# CELL ********************

emissionsDf = spark.read.load(SOURCE_ABFS_PATH + '/*/emissions_*.parquet', format='parquet')
emissionsDf.write.format("delta").mode('overwrite').saveAsTable(TARGET_LAKEHOUSE_NAME + ".azure_emissions")
