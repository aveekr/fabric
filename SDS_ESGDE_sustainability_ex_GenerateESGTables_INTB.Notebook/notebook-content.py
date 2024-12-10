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
# META           "id": "82c59f84-614f-450a-9639-8161758354ef"
# META         },
# META         {
# META           "id": "b1c7648a-5064-48b2-a7c9-1130bfb4cac6"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Overview
# 
# This notebook helps in creating empty MCFS ESG data model tables for specified sustainability area. This can be helpful while exploring the ESG data model tables. \
# You can also explore the ESG data model using the schema file.

# MARKDOWN ********************

# ### Parameters
# 
# ___TARGET_LAKEHOUSE___ : Name of the Lakehouse where the tables needs to be created.\
# ___CONFIG_AND_DEMO_DATA_LAKEHOUSE_NAME___ : Name of the Lakehouse storing the config files.\
# ___SELECTED_BUSINESS_AREA___ : Select the business area to generate the ESG tables . Select from the following list:
# 
# - GHG Emissions
# - Waste Sustainability
# - Water Sustainability
# - Environmental Social and Govenance
# - Circularity
# - Business Management
# - Party

# CELL ********************

TARGET_LAKEHOUSE = "SDS_ESGDE_sustainability_ex_ProcessedESGData_LH"
CONFIG_AND_DEMO_DATA_LAKEHOUSE_NAME = "SDS_ESGDE_sustainability_ex_ConfigAndDemoData_LH"
SELECTED_BUSINESS_AREA = "Water Sustainability"

# MARKDOWN ********************

# ### Run utility notebook

# CELL ********************

%run "SDS_ESGDE_sustainability_ex_Utilities_INTB"

# MARKDOWN ********************

# ### Initialize Configuration and Demo data for the capability

# CELL ********************

initialize_config_and_demo_data(Capability.ESGDataEstate)

# MARKDOWN ********************

# Import required libraries

# CELL ********************

import json
from notebookutils import mssparkutils
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import IntegerType, StringType, BooleanType, DateType, TimestampType, BinaryType, DecimalType, LongType

spark.conf.set("spark.sql.caseSensitive", "true")

# MARKDOWN ********************

# Derived parameters and constants

# CELL ********************

config_and_demo_data_lakehouse_abfs_path = mssparkutils.lakehouse.get(CONFIG_AND_DEMO_DATA_LAKEHOUSE_NAME).get("properties").get("abfsPath")
ESGLakeSchemaPath = f"{config_and_demo_data_lakehouse_abfs_path}/Files/{Capability.ESGDataEstate.value}/Config/ESGSchema.json"

ListedBusinessAreas = [
    "GHG Emissions",
    "Waste Sustainability",
    "Water Sustainability",
    "Environmental Social and Govenance",
    "Circularity",
    "Business Management",
    "Party"
]

# MARKDOWN ********************

# ### Function to create table from schema
# Function creates empty table without any rows from the ESG schema

# CELL ********************

# Define a mapping of data types
data_type_mapping = {
    'integer': IntegerType(),
    'string': StringType(),
    'boolean': BooleanType(),
    'date': DateType(),
    'timestamp': TimestampType(),
    'binary': BinaryType(),
    'decimal': DecimalType(18, 2),
    'long': LongType()
}

def ReadFile(filePath):
    rdd = spark.sparkContext.wholeTextFiles(filePath)
    return rdd.collect()[0][1]

# Load schema from Model JSON file
def loadSchemaFromModelJson(table_info):
    # Extract columns information
    columns = table_info['storageDescriptor']['columns']

    # Define the schema for the DataFrame
    fields = []

    # Iterate through the columns and create DataFrame schema fields
    for column in columns:
        name = column['name']
        datatype = column['originDataTypeName']['typeName']
        is_nullable = column['originDataTypeName']['isNullable']

        # Define additional attributes for 'date' and 'timestamp' data types
        additional_attributes = {}
        if datatype == 'date':
            # Extract 'dateFormat' for 'date' data type
            additional_attributes['dateFormat'] = column['originDataTypeName']['properties'].get('dateFormat')
        elif datatype == 'timestamp':
            # Extract 'timestampFormat' for 'timestamp' data type
            additional_attributes['timestampFormat'] = column['originDataTypeName']['properties'].get('timestampFormat')
        elif datatype == 'string':
            length = column['originDataTypeName'].get('length')
            # Set a default length of 256 for 'string' data type if length is not defined
            if length is None:
                length = 256
            additional_attributes['length'] = length
        elif datatype == 'decimal':
            # Extract 'precision,' and 'scale' for 'decimal' data type
            precision = column['originDataTypeName'].get('precision')
            scale = column['originDataTypeName'].get('scale')
            if precision is not None and scale is not None:
                data_type_mapping[datatype] = DecimalType(precision, scale)

        # Append the column information to the schema
        if datatype in data_type_mapping:
            fields.append(StructField(name, data_type_mapping[datatype], is_nullable, additional_attributes))

    # Define additional fields for tracking the source information
    additional_fields = [  
        StructField("SourceModifiedOn", TimestampType(), True, {"timestampFormat": "yyyy-MM-ddTHH:mm:ssZ"}),  
        StructField("SourceTable", StringType(), True)  
    ]  
    fields.extend(additional_fields)

    # Create the DataFrame schema
    schema = StructType(fields)
    return schema

# MARKDOWN ********************

# ### Create ESG tables for the selected business area
# For all the tables of the selected business area, reads the schema for the table and generates empty table in the *processed ESG data* Lakehouse

# CELL ********************

esgSchema = json.loads(ReadFile(ESGLakeSchemaPath))
targetTables = spark.catalog.listTables(TARGET_LAKEHOUSE)

# Iterate through each table in metadata and create tables as required
for table_info in esgSchema:
    if SELECTED_BUSINESS_AREA in table_info['properties']['fromBusinessAreas'].split(',') and table_info['name'] not in targetTables:
        schema = loadSchemaFromModelJson(table_info)    
        df = spark.createDataFrame([], schema)
        df.write\
            .format("delta")\
            .mode("ignore")\
            .saveAsTable(table_info['name'])
