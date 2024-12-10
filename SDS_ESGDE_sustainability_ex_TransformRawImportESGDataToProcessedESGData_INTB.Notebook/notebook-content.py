# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b1c7648a-5064-48b2-a7c9-1130bfb4cac6",
# META       "default_lakehouse_name": "SDS_ESGDE_sustainability_ex_ProcessedESGData_LH",
# META       "default_lakehouse_workspace_id": "33b35682-feda-47b7-967b-3905434352d7",
# META       "known_lakehouses": [
# META         {
# META           "id": "b1c7648a-5064-48b2-a7c9-1130bfb4cac6"
# META         },
# META         {
# META           "id": "82c59f84-614f-450a-9639-8161758354ef"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Overview
# 
# Once you have loaded emission, water and waste Microsoft Sustainability Manager (MSM) data to the ‘IngestedRawData’ Lakehouse as tables, then unify and harmonize the data to processed data as per the Microsoft Cloud for Sustainability (MCFS) ESG data model.
# 
# Note, the MSM data to MCFS ESG data model transformation is a 2-stage process.\
# This notebook constitutes stage 2 of transformation.\
# The transformed data is stored as tables in the ‘ProcessedESGData’ Lakehouse.
# 
# <u>Prerequisite:</u>\
# Prior to executing this notebook, ensure that ‘TransformMSMDataToProcessedESGDataStage1’ is successfully executed.
# 
# To explore the ESG data model schema you refer to the ‘ESGSchema’ schema file in the Config folder of the ‘ConfigAndDemoData’ Lakehouse.\
# For more information [click here](https://go.microsoft.com/fwlink/?linkid=2288320) to view ESG data estate documentation.


# MARKDOWN ********************

# ### Parameters
# ___CONFIG_AND_DEMO_DATA_LAKEHOUSE_NAME___ : Lakehouse storing the configuration files

# CELL ********************

CONFIG_AND_DEMO_DATA_LAKEHOUSE_NAME = "SDS_ESGDE_sustainability_ex_ConfigAndDemoData_LH"

# MARKDOWN ********************

# Install required packages

# CELL ********************

import re
import os
import shutil
from notebookutils import mssparkutils
import zipfile
from pathlib import Path 

# MARKDOWN ********************

# ### Run utility notebook

# CELL ********************

%run "SDS_ESGDE_sustainability_ex_Utilities_INTB"

# MARKDOWN ********************

# ### Initialize Configuration and Demo data for the capability

# CELL ********************

initialize_config_and_demo_data(Capability.ESGDataEstate)

# MARKDOWN ********************

# Copy the python wheel packages to the mounted storage

# CELL ********************

def MountAndCopyPackages(source_path : str):
    packages_mount_name = "/esglake_packages"
    temp_packages_folder_path = "tmp/esglake_packages"
    try:
        mssparkutils.fs.unmount(packages_mount_name)
        mssparkutils.fs.mount(source_path, packages_mount_name)
        packages_mount_local_path = mssparkutils.fs.getMountPath(packages_mount_name)
        
        # Copy the packages to a temp folder: `tmp/esglake_packages`
        if os.path.exists(temp_packages_folder_path):
            shutil.rmtree(temp_packages_folder_path)
        shutil.copytree(f"{packages_mount_local_path}", f"{temp_packages_folder_path}", dirs_exist_ok=True)
        print(f"Successfully mounted and copied the packages to: `{temp_packages_folder_path}`")
    except Exception as ex:
        print(f"An error occurred while mounting the source path: {ex}")
        raise

# CELL ********************

# Construct absolute paths
config_and_demo_data_lakehouse_abfs_path = mssparkutils.lakehouse.get(CONFIG_AND_DEMO_DATA_LAKEHOUSE_NAME).get("properties").get("abfsPath")
data_manager_packages_path = f"{config_and_demo_data_lakehouse_abfs_path}/Files/{Capability.ESGDataEstate.value}/Packages"

# Mount the datamanager config path
enable_packages_mount = True
if enable_packages_mount:
    MountAndCopyPackages(source_path=data_manager_packages_path)

# MARKDOWN ********************

# ##### Install custom libraries
# Installs the libraries provided by this capability and stored as wheel files in the *configs* Lakehouse at path *Files/Packages/*

# CELL ********************

#DTT
! pip install tmp/esglake_packages/dtt-0.2.0.591-py3-none-any.whl

# Install typing extensions (for RMT)
! pip install typing_extensions  

# Restart the Python session to apply the installed packages
mssparkutils.session.restartPython()

# MARKDOWN ********************

# ### Run utility notebook

# CELL ********************

%run "SDS_ESGDE_sustainability_ex_Utilities_INTB"

# MARKDOWN ********************

# ### Parameters
# ___CONFIG_AND_DEMO_DATA_LAKEHOUSE_NAME___ : Lakehouse storing the configuration files\
# ___TARGET_DATALAKEHOUSE_NAME___ : *processed ESG data* Lakehouse\
# ___SOURCE_DATALAKE_NAME___ : Name of the Lakehouse storing the *processed ESG data* Lakehouse

# CELL ********************

CONFIG_AND_DEMO_DATA_LAKEHOUSE_NAME = "SDS_ESGDE_sustainability_ex_ConfigAndDemoData_LH"
TARGET_DATALAKEHOUSE_NAME = "SDS_ESGDE_sustainability_ex_ProcessedESGData_LH"
SOURCE_DATALAKE_NAME = "SDS_ESGDE_sustainability_ex_ProcessedESGData_LH"


# MARKDOWN ********************

# Derived parameters

# CELL ********************

config_and_demo_data_lakehouse_abfs_path = mssparkutils.lakehouse.get(CONFIG_AND_DEMO_DATA_LAKEHOUSE_NAME).get("properties").get("abfsPath")
target_lakehouse_abfs_path = mssparkutils.lakehouse.get(TARGET_DATALAKEHOUSE_NAME).get("properties").get("abfsPath")

RMT_MAPPING_FOLDER_PATH = [f"{config_and_demo_data_lakehouse_abfs_path}/Files/{Capability.ESGDataEstate.value}/ReferenceData/Mapping"]
CONFIG_PATH =  f"{config_and_demo_data_lakehouse_abfs_path}/Files/{Capability.ESGDataEstate.value}/Config"

adapter = f"{CONFIG_PATH}/TransformRawImportESGDataToProcessedESGDataAdapter.json"
ESGSchemaSemantics = f"{CONFIG_PATH}/ESGSchemaSemantics.json"
ESGSchemaSemanticsConfig = f"{CONFIG_PATH}/ESGSchemaSemanticsConfig.json"
ESGSchema = f"{CONFIG_PATH}/ESGSchema.json"
ESGSchemaConfig = f"{CONFIG_PATH}/ESGSchemaConfig.json"
envConfig = f"{CONFIG_PATH}/environment_temp.json"

DMF_SPEC_PATH = f"{CONFIG_PATH}/dmfSpec_stage2.json"
RMT_SPEC_PATH = f"{CONFIG_PATH}/rmtSpec_stage2.json"

# METADATA ********************

# META {}

# MARKDOWN ********************

# ### Update adapter config file
# Updates the stage-2 adapter config file ( */Files/Config/TransformRawImportESGDataToProcessedESGDataAdapter.json* ) in *configs* Lakehouse with references to the config, source and target Lakehouse paths. This will be used for the data transformation from source to target using the configs provided.

# CELL ********************

environment = f"""
{{
  "storage": {{
    "target": {{
      "entities": {{
        "default": {{
          "location": "{target_lakehouse_abfs_path}/Tables",
          "format": "delta"
        }}
      }}
    }},
    "secondary_lake": {{
      "location": "{config_and_demo_data_lakehouse_abfs_path}/Files/{Capability.ESGDataEstate.value}/internal/dtt"
    }}
  }}
}}
"""

mssparkutils.fs.put(envConfig, environment, True)
if not mssparkutils.fs.exists(f"{config_and_demo_data_lakehouse_abfs_path}/Files/{Capability.ESGDataEstate.value}/internal/dtt"):
  mssparkutils.fs.mkdirs(f"{config_and_demo_data_lakehouse_abfs_path}/Files/{Capability.ESGDataEstate.value}/internal/dtt")
  
adapterContent = spark.read.text(adapter).collect()
adapterContent = "".join([row.value for row in adapterContent])
adapterContent = adapterContent.replace("%%lakeDBName%%", SOURCE_DATALAKE_NAME)

mssparkutils.fs.put(adapter, adapterContent, True)

# MARKDOWN ********************

# ### Compile configurations for RMT and DMF libraries
# Compiles the configuration files for RMT and DMF libraries.
# 
# __RMT (ReferenceData Mapping Toolkit)__ generates the cache for the reference tables.\
# __DMF (Data Movement Framework)__ transforms the data from source schema to target schema.

# CELL ********************

# Compile configurations for RMT
from configuration_compiler.api import persist_rmt_spec
persist_rmt_spec(
    spark = spark,
    out_path= RMT_SPEC_PATH, 
    target_db_semantics_file_location = ESGSchemaSemantics, 
    env_config_file_location = envConfig,
    adaptor_file_location= adapter,
    target_db_schema_file_location=ESGSchema,
    db_schema_config_location=ESGSchemaConfig
)
# Compile configurations for DMF
from configuration_compiler.api import persist_dtt_spec
persist_dtt_spec(
    spark= spark,
    out_path= DMF_SPEC_PATH,
    dmf_adaptor_file_location=adapter,
    target_db_semantics_file_location=ESGSchemaSemantics,
    target_db_semantics_config_file_location=ESGSchemaSemanticsConfig,
    target_db_schema_file_location=ESGSchema,
    db_schema_config_location=ESGSchemaConfig,
    env_config_file_location=envConfig,
)

# MARKDOWN ********************

# ### Load Reference Tables

# CELL ********************


from rmt.runners.fabric_runner import FabricRunner as RMTFabricRunner
RMTFabricRunner.create_reference_data_tables(
    spark=spark, 
    rmt_spec_path=RMT_SPEC_PATH, 
    target_path = f"{target_lakehouse_abfs_path}/Tables",
    staging_reference_data_folder_path = f"{config_and_demo_data_lakehouse_abfs_path}/Files/{Capability.ESGDataEstate.value}/ReferenceData"
)

# MARKDOWN ********************

# ### [Optional] Enable Azure Application Insights for logging
# This will enable logs instrumentation to the [Azure Application Insights](https://learn.microsoft.com/en-us/azure/azure-monitor/app/app-insights-overview#application-insights-overview). Please get the [Instrumentation Key](https://learn.microsoft.com/en-us/azure/azure-monitor/app/sdk-connection-string) and uncomment the line mentioned to enable to logs.

# CELL ********************

import logging
from opencensus.ext.azure.log_exporter import AzureLogHandler
logging.getLogger().handlers

# Optional: Uncomment below line and update instrumentation key to set Azure Application Insights instrumentation key for logging.
# spark.conf.set("spark.dtt.application_insights.instrumentation_key", "[AzureApplicationInsights_InstrumentationKey]")

# MARKDOWN ********************

# ### Run RMT
# Runs RMT (ReferenceData Mapping Toolkit) which generates the cache for the reference tables.

# CELL ********************

from rmt.runners.fabric_runner import FabricRunner as RMTFabricRunner
RMTFabricRunner.create_reference_values_mapping(
    spark=spark, 
    rmt_spec_path=RMT_SPEC_PATH, 
    ordered_mapping_definitions_folders=RMT_MAPPING_FOLDER_PATH
)

# MARKDOWN ********************

# ### Run DMF
# Runs DMF (Data Movement Framework) which transforms the data from source schema to target schema.

# CELL ********************

# MAGIC %%capture
# MAGIC 
# MAGIC spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
# MAGIC 
# MAGIC from dmf.runners.fabric.fabric_runner import FabricRunner as DMFFabricRunner
# MAGIC DMFFabricRunner.run(spark, DMF_SPEC_PATH)
