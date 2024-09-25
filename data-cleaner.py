import json
import requests
import re
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import psycopg2
from bs4 import BeautifulSoup
import pandas as pd
from db.db_config import DatabaseConfig
from time_distance import extract_time_and_distance
from minimums import extract_minimums

# Path to JARs
jdbc_jar = "/app/jars/postgresql-42.2.20.jar"

file_path = "hsd_reference.xlsx"

# Combine all the JAR paths into a single string
jars = f"{jdbc_jar}"

# Load configuration from JSON file
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

# Config
url = config["url"]
file_regex = config["file_regex"]
sheets = config["sheets"]

# Mapping of string data types to PySpark types
data_type_map = {
    "StringType": "StringType()",
    "DoubleType": "DoubleType()",
    "IntegerType": "IntegerType()"
}


# Step 3: Initialize PySpark session
spark = SparkSession.builder \
    .appName("HSD Reference File Processing") \
    .config("spark.jars", jars) \
    .getOrCreate()



# Download the page content
response = requests.get(url)
page_content = response.text

# Use BeautifulSoup to parse the HTML
soup = BeautifulSoup(page_content, 'html.parser')

# Find the link that matches the regex
file_url = None
for link in soup.find_all('a', href=True):
    if re.search(file_regex, link.text):
        file_url = link['href']
        break

#Check if file_url was found and construct the full URL if necessary
if file_url:
    # Construct the full URL
    if not file_url.startswith('http'):
        file_url = f"https://www.cms.gov{file_url}"

# Download the Excel file
    try:
        excel_response = requests.get(file_url)
        excel_response.raise_for_status()  # Raise an error for bad responses
        excel_file_path = file_path

        with open(excel_file_path, 'wb') as file:
            file.write(excel_response.content)
        print("File downloaded successfully.")

    except requests.exceptions.RequestException as e:
        print(f"Error downloading the file: {e}")
else:
    print("Error: No matching files found for the regex pattern.")

time_distance_df = extract_time_and_distance(file_path)
minimums_df = extract_minimums(file_path,time_distance_df)


spark = SparkSession.builder \
    .appName("HSD File Processing") \
    .getOrCreate()

# Convert the Pandas DataFrame to Spark DataFrame
df_time_and_distance = spark.createDataFrame(time_distance_df)
df_minimums = spark.createDataFrame(minimums_df)

# Initialize the DatabaseConfig to load DB configurations
db_config = DatabaseConfig()
# Retrieve JDBC URL and connection properties
jdbc_url = db_config.get_jdbc_url()
connection_properties = db_config.get_connection_properties()


# Save the Spark DataFrame to PostgreSQL using the JDBC connection
df_time_and_distance.write \
    .jdbc(url=jdbc_url, table="time_and_distance", mode="overwrite", properties=connection_properties)
df_minimums.write \
    .jdbc(url=jdbc_url, table="minimums", mode="overwrite", properties=connection_properties)


spark.stop()
