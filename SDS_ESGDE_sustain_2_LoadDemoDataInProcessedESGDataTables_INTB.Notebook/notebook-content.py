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
# META           "id": "0d33f4cc-45da-4bb7-bee9-4ca07a1696f0"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Overview
# 
# Demo data for emissions, water, waste, social and governance sustainability areas in the MCFS ESG data model schema is deployed along with the capability in your workspace in the Demo data folder of the ‘ConfigAndDemoData’ Lakehouse.\
# You can load this demo data as tables in the ‘ProcessedESGData’ lakehouse for populating the dimensional fact tables, metric tables and for exploring the pre-built ‘CSRDMetricsReport’.\
# This notebook can be used for loading the demo data as tables in the MCFS ESG data model schema in the ‘ProcessedESGData’ lakehouse.
# 
# Note: -
# 1.	Demo data is for illustration purposes only. No real association is intended or inferred.
# 2.	[Caution] In case you have loaded certain ESG data model tables with your data in the ‘ProcessedESGData’ lakehouse that overlap with the tables present in the demo data then the overlapping tables will be overwritten.
# 
# For more information [click here](https://go.microsoft.com/fwlink/?linkid=2288320) to view ESG data estate documentation.

# MARKDOWN ********************

# ### Run utility notebook

# CELL ********************

%run "SDS_ESGDE_sustain_2_Utilities_INTB"

# MARKDOWN ********************

# ### Parameters
# 
# ___CONFIG_AND_DEMO_DATA_LAKEHOUSE_NAME___ : Lakehouse name containing the demo data zip.\
# ___SOURCE_FOLDER_FILE_API_PATH___ : API path to the Lakehouse folder containing the demo data.\
# ___TARGET_LAKEHOUSE_NAME___ : Name of the lakehouse where the processed ESG data is stored.

# CELL ********************

CONFIG_AND_DEMO_DATA_LAKEHOUSE_NAME = "SDS_ESGDE_sustain_2_ConfigAndDemoData_LH"
SOURCE_FOLDER_FILE_API_PATH = f"/lakehouse/default/Files/{Capability.ESGMetrics.value}/DemoData"
TARGET_LAKEHOUSE_NAME = 'SDS_ESGDE_sustain_2_ProcessedESGData_LH'

# MARKDOWN ********************

# Import required libraries and set spark configurations

# CELL ********************

import zipfile
import os
import shutil
from pathlib import Path 
from notebookutils import mssparkutils

spark.conf.set("spark.sql.caseSensitive", "true")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")

# MARKDOWN ********************

# ##### Derived parameters

# CELL ********************

config_and_demo_data_lakehouse_abfs_path = mssparkutils.lakehouse.get(CONFIG_AND_DEMO_DATA_LAKEHOUSE_NAME).get("properties").get("abfsPath")
source_folder_abfs_path = f"{config_and_demo_data_lakehouse_abfs_path}/Files/{Capability.ESGMetrics.value}/DemoData"

# MARKDOWN ********************

# ### Initialize Configuration and Demo data for the capability

# CELL ********************

initialize_config_and_demo_data(Capability.ESGMetrics)

# MARKDOWN ********************

# Loads demo data as Lakehouse tables

# CELL ********************

for table in os.listdir(SOURCE_FOLDER_FILE_API_PATH):
    tableFolderABFSPath = os.path.join(f"{source_folder_abfs_path}", table)
    df = spark.read.format("delta").load(tableFolderABFSPath)
    df.write.format("delta").mode('overwrite').saveAsTable(TARGET_LAKEHOUSE_NAME + '.' + table)
