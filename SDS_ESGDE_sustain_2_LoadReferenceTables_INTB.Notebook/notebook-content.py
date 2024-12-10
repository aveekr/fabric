# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0d33f4cc-45da-4bb7-bee9-4ca07a1696f0",
# META       "default_lakehouse_name": "SDS_ESGDE_sustain_2_ProcessedESGData_LH",
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
# Reference data are standardized, predefined values used to categorize, classify, or represent specific information in data systems like unit of measure, country etc. Following are the SDSF reference tables which are divided into two categories - Common and Sustainability.
# 
# Common
# - AssetType
# - BusinessMetric
# - CalculationAlgorithm
# - CalculationAlgorithmBasis
# - Country
# - Currency
# - EmployeePartyRelationshipType
# - EventType
# - FacilityType
# - FuelType
# - Gender
# - IncidentType
# - ItemType
# - LocationType
# - MetricPurpose
# - PartyOrganizationType
# - PartyRelationshipType
# - PartyType
# - ProcessType
# - SampleTestResultType
# - SampleTestType
# - StorageContainerType
# - UnitOfMeasure
# - UnitOfMeasureConversion
# - UnitOfMeasureType
# 
# Sustainability
# - GreenhouseGas
# - GreenhouseGasEmissionsCategory
# - GreenhouseGasEmissionsPurposeType
# - GreenhouseGasEmissionsScope
# - GreenhouseGasSourceType
# - PurchasedEnergyType
# - SampleTestCategory
# - SustainableContentType
# - WasteCategory
# - WasteDiversionMethod
# - WasteMaterialType
# - WasteStream
# - WaterRiskIndex
# - WaterRiskType
# - WaterSourceType
# - WaterType
# - WaterUtilizationType
# 
# 
# You can view the reference data files from the ‘Reference Data’ folder in the ‘ConfigAndDemoData’ lakehouse.\
# This notebook is used for loading the reference data to processedESGData tables.
# 
# For more information [click here](https://go.microsoft.com/fwlink/?linkid=2288320) to view ESG data estate documentation.

# MARKDOWN ********************

# ### Run utility notebook

# CELL ********************

%run "SDS_ESGDE_sustain_2_Utilities_INTB"

# MARKDOWN ********************

# ### Parameters
# ___CONFIG_AND_DEMO_DATA_LAKEHOUSE_NAME___: Lakehouse with the config files.

# CELL ********************

CONFIG_AND_DEMO_DATA_LAKEHOUSE_NAME = "SDS_ESGDE_sustain_2_ConfigAndDemoData_LH"

# MARKDOWN ********************

# Import required libraries and set spark configurations

# CELL ********************

import json
import os
from pyspark.sql.functions import concat_ws, collect_list, input_file_name
from notebookutils import mssparkutils

spark.conf.set("spark.sql.caseSensitive", "true")

# MARKDOWN ********************

# ##### Derived parameters

# CELL ********************

config_and_demo_data_lakehouse_abfs_path = mssparkutils.lakehouse.get(CONFIG_AND_DEMO_DATA_LAKEHOUSE_NAME).get("properties").get("abfsPath")

target_schema_files_abfs_path = f'{config_and_demo_data_lakehouse_abfs_path}/Files/{Capability.ESGDataEstate.value}/Config/ESGSchema.json'
reference_tables_source_folder_abfs_path = f'{config_and_demo_data_lakehouse_abfs_path}/Files/{Capability.ESGDataEstate.value}/ReferenceData/Data/'

# MARKDOWN ********************

# ### Initialize Configuration and Demo data for the capability

# CELL ********************

initialize_config_and_demo_data(Capability.ESGDataEstate)

# MARKDOWN ********************

# ### Load data as Lakehouse tables
# Loads the reference table's data from *configs* Lakehouse to the *processed ESG data* Lakehouse as tables.

# CELL ********************

# Read reference tables.
targetTables = {}

referenceTablesDf = spark.read.text(paths=reference_tables_source_folder_abfs_path, recursiveFileLookup=True)
referenceTablesDfWithFilename = referenceTablesDf.withColumn("FileName", input_file_name())
referenceTablesDfWithFilenameNContent = referenceTablesDfWithFilename.groupBy('FileName').agg(concat_ws("\n", collect_list(referenceTablesDfWithFilename.value)).alias("file_content"))

for content in referenceTablesDfWithFilenameNContent.collect():
    referenceTable = json.loads(content.file_content)
    targetTables[referenceTable['adrmTableName']] = referenceTable

targetTableNames = targetTables.keys()

# Load Target Schema file.
rdd = spark.sparkContext.wholeTextFiles(target_schema_files_abfs_path)
ESGSchema = json.loads(rdd.collect()[0][1])

# Construct Reference tables
referenceTableEntities = []
for entity in ESGSchema:
    if entity['entityType'] == 'TABLE' and entity['name'] in targetTableNames:
        
        # Parse the table content
        targetTable = targetTables[entity['name']]
        fieldMap = {}
        for fieldMapEntity in targetTable['fieldMapping']:
            fieldMap[fieldMapEntity['referenceFieldName']] = fieldMapEntity['adrmFieldName']

        # transform data to new fields
        transformedEntity = []
        for data in targetTable['data']:
            transformedData = {}
            for key in data.keys():
                transformedData[fieldMap[key]] = data[key]
            transformedEntity.append(json.dumps(transformedData))

        columns = entity['storageDescriptor']['columns']
        entitySchema = []
        for i, column in enumerate(columns):
            type = column['originDataTypeName']['typeName']
            if type == "long":
                type = "bigint"
            elif type == "decimal":
                precision = column['originDataTypeName'].get('precision', 10)
                scale = column['originDataTypeName'].get('scale', 0)
                type = f"decimal({precision},{scale})"
            entitySchema.append('{0} {1}'.format(column['name'], type.upper()))
        
        entitySchemaString = ','.join(entitySchema)
        df = spark.read.schema(entitySchemaString).json(sc.parallelize(transformedEntity))
        df.write\
            .format("delta")\
            .mode("overwrite")\
            .saveAsTable(entity['name'])

