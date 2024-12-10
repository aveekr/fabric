# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# ## Utility functions

# MARKDOWN ********************

# ### Import necessary libraries

# CELL ********************

import ast
import json
from datetime import datetime, date
from functools import reduce

import pandas as pd
from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, MapType, 
    IntegerType, DoubleType, TimestampType, BooleanType, DateType, 
    LongType, BinaryType, DecimalType
)
from pyspark.sql.window import Window

import sempy.fabric as sempy

import zipfile
from notebookutils import mssparkutils
import shutil, os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Set Spark configuration

# CELL ********************

spark.conf.set("spark.sql.caseSensitive", "true")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.conf.set("spark.sql.jsonGenerator.ignoreNullFields", "false")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Define variables

# CELL ********************

gold_lakehouse_name = "SDS_ESGDE_sustain_2_ComputedESGMetrics_LH"
silver_lakehouse_name = "SDS_ESGDE_sustain_2_ProcessedESGData_LH"
config_lakehouse_name = "SDS_ESGDE_sustain_2_ConfigAndDemoData_LH"

config_lakehouse_path = mssparkutils.lakehouse.get(config_lakehouse_name).get("properties").get("abfsPath")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Utility function to save a dataframe to lakehouse table

# CELL ********************

def save_dataframe_to_lakehouse(df, lakehouse_name, table_name):  
    """  
    Save a DataFrame to a specified table in the Lakehouse in Delta format.  
  
    Args:  
        df (DataFrame): DataFrame to be saved.  
        lakehouse_name (str): Name of the Lakehouse.  
        table_name (str): Name of the table where the DataFrame should be saved.  
    """  
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{lakehouse_name}.{table_name}")  


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Utility function to save a dataframe to lakehouse file as JSON

# CELL ********************

def save_dataframe_to_lakehouse_as_json(df, file_path):  
    """  
    Save a DataFrame to a specified file path in JSON format in the Lakehouse.  
  
    Args:  
        df (DataFrame): DataFrame to be saved.  
        file_path (str): Path where the DataFrame should be saved.  
    """  
    df.coalesce(1).write.format("json").mode("overwrite").save(file_path)  


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Utility function to read a JSON file

# CELL ********************

def read_json(file_path):  
    """  
    Read a JSON file from the specified file path and return its content as a dictionary.  
  
    Args:  
        file_path (str): Path to the JSON file.  
  
    Returns:  
        dict: Parsed JSON content as a dictionary.  
    """  
    # Read the file as text  
    file = spark.read.text(file_path)  
      
    # Concatenate all lines into a single string  
    file_text = "".join([row['value'] for row in file.collect()])  
      
    # Parse the JSON string and return it as a dictionary  
    return json.loads(file_text)  


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Initialize configuration and demo data for the capability 

# CELL ********************

from enum import Enum

class Capability(Enum):
    ESGDataEstate = "ESGDataEstate"
    ESGMetrics = "ESGMetrics"
    EnvironmentalMetricsAndAnalytics = "EnvironmentalMetricsAndAnalytics"
    MicrosoftAzureEmissionsInsights = "MicrosoftAzureEmissionsInsights"
    SocialAndGovernanceMetricsAndReports = "SocialAndGovernanceMetricsAndReports"

def extract_zip_file(lakehouse_name, file_name_without_extension, overwrite=False):
    """
    Extracts the contents of a zip file in the lakehouse to the same folder.
    Args:
        lakehouse_name (str): Name of the lakehouse
        file_name_without_extension (str): Name of the zip file without the extension
        overwrite (bool): Flag to overwrite the existing files in the lakehouse
    """

    # Mount the lakehouse
    lakehouse = mssparkutils.lakehouse.get(lakehouse_name)
    mssparkutils.fs.mount(lakehouse.get("properties").get("abfsPath"), f"/{lakehouse_name}")

    # Get the local path where the lakehouse is mounted
    path = mssparkutils.fs.getMountPath(f"/{lakehouse_name}")

    # Check if the zip file exists in the lakehouse and overwrite only if the overwrite flag is set to True
    if overwrite == False and os.path.exists(f"{path}/Files/{file_name_without_extension}"):
        # Unmount the lakehouse
        mssparkutils.fs.unmount(f"/{lakehouse_name}")
        return

    # Extract the contents of the zip file to a temporary folder in the lakehouse
    with zipfile.ZipFile(f"{path}/Files/{file_name_without_extension}.zip", 'r') as zip_ref: 
        zip_ref.extractall(f"{path}/Files/_{file_name_without_extension}")
    
    # Move the extracted files to the same folder as the zip filename once unzip is successful
    if os.path.exists(f"{path}/Files/{file_name_without_extension}"):
        shutil.rmtree(f"{path}/Files/{file_name_without_extension}")

    shutil.move(f"{path}/Files/_{file_name_without_extension}", f"{path}/Files/{file_name_without_extension}")

    # Unmount the lakehouse
    mssparkutils.fs.unmount(f"/{lakehouse_name}")


def initialize_config_and_demo_data(capability, overwrite=False):
    """  
    Initialize the configuration and demo data for the specified capability.  
    Args:  
        capability (Enum): Enum value representing the capability for which the configuration and demo data should be initialized.
        overwrite (bool): Flag to overwrite the existing files in the lakehouse. 
    """  
    
    # Extract config and demo data of the specified capability
    extract_zip_file(config_lakehouse_name, capability.value, overwrite)

    # Extract config and demo data of the dependent ESG Data estate capability
    extract_zip_file(config_lakehouse_name, Capability.ESGDataEstate.value, overwrite)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Utility function to read an ESG data model table and return empty dataframe if it doesn't exists

# CELL ********************

esg_schema_file_path = f"{config_lakehouse_path}/Files/{Capability.ESGDataEstate.value}/Config/ESGSchema.json"

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

def get_schema(table_info):
    """
    Helper function to get ESG table schema.
    Args:
        table_info (dict): Information about the table as per ESG Schema JSON.

    Returns:
        StructType: Schema for the DataFrame.
    """
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
    return StructType(fields)

def load_or_initialize_esg_table(table_name):
    """
    Read the specified table or create empty table if it doesn't exist.

    Args:
        table_name (str): Name of the ESG table.

    Returns:
        DataFrame: ESG table.
    """
    # Read the ESG Schema JSON file
    esg_schema = read_json(esg_schema_file_path)
    table_info = [table for table in esg_schema if table["name"] == table_name]
    if len(table_info) == 0:
        raise Exception(f"Table {table_name} does not exist in ESG schema.")
    table_info = table_info[0]

    try:
        return spark.read.table(f"{silver_lakehouse_name}.{table_name}")
    except:
        print(f"Table '{table_name}' does not exist in the '{silver_lakehouse_name}' Lakehouse. Initializing an empty DataFrame.")
        return spark.createDataFrame([], get_schema(table_info))
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
