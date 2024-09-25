import json
import requests
import re
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import psycopg2
from bs4 import BeautifulSoup
import pandas as pd

# Path to JARs
jdbc_jar = "/app/jars/postgresql-42.2.20.jar"

# Combine all the JAR paths into a single string
jars = f"{jdbc_jar}"

# Load configuration from JSON file
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

# Config
url = config["url"]
file_regex = config["file_regex"]
sheets = config["sheets"]

# DB Connection
db_host = config["db_connection"]["host"]
db_name = config["db_connection"]["db"]
db_user = config["db_connection"]["user"]
db_password = config["db_connection"]["password"]
db_port = config["db_connection"]["port"]

# Mapping of string data types to PySpark types
data_type_map = {
    "StringType": "StringType()",
    "DoubleType": "DoubleType()",
    "IntegerType": "IntegerType()"
}
# # Set options to display all rows and columns
# pd.set_option('display.max_rows', None)  # Display all rows
# pd.set_option('display.max_columns', None)  # Display all columns


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
        excel_file_path = "time_and_distance.xlsx"

        with open(excel_file_path, 'wb') as file:
            file.write(excel_response.content)
        print("File downloaded successfully.")

    except requests.exceptions.RequestException as e:
        print(f"Error downloading the file: {e}")
else:
    print("Error: No matching files found for the regex pattern.")


# Load the Excel sheet directly using Pandas
df_pandas = pd.read_excel("time_and_distance.xlsx", sheet_name="Provider Time & Distance", header=None)
# Step 3: Drop the first row if it contains unwanted text
df_pandas_data_frame_2 = df_pandas.drop(index=[0,1,2,3])
df_pandas_data_frame_1 = df_pandas.drop(index=[0,1,2,3])
headings_frame_1 = df_pandas.loc[[1]]
headings_frame_2 = df_pandas.loc[1:3]

# Step 4: Reset the index after dropping the row
df_pandas_data_frame_1.reset_index(drop=True, inplace=True)
df_pandas_data_frame_2.reset_index(drop=True, inplace=True)
headings_frame_1.reset_index(drop=True, inplace=True)
headings_frame_2.reset_index(drop=True, inplace=True)

# Step 4: Select only the first 5 columns
df_pandas_data_frame_1 = df_pandas_data_frame_1.iloc[:,:5]
headings_frame_1 = headings_frame_1.iloc[:, :5]
df_pandas_data_frame_2 = df_pandas_data_frame_2.iloc[:, 5:]
headings_frame_2 = headings_frame_2.iloc[:,5:]


records = []




# Logic to extract time and distance for each row in dataframe_1
for i in range(len(df_pandas_data_frame_1)):
    for j in range(df_pandas_data_frame_2.shape[1]):
       if (j % 2 == 0):
#         if pd.notnull(df_cleaned_frame_2.iloc[i, j]):
        specialty = headings_frame_2.iloc[0, j]  # Specialty name is in row 0
        specialty_code = headings_frame_2.iloc[1, j]  # Specialty code is in row 1

        if j + 1 < df_pandas_data_frame_2.shape[1]:
             time = df_pandas_data_frame_2.iloc[i, j]  # Assuming time is in the current column
             distance = df_pandas_data_frame_2.iloc[i, j + 1]  # Distance in the next column

        records.append({
                'county': df_pandas_data_frame_1.iloc[i, 0],
                'state': df_pandas_data_frame_1.iloc[i, 1],
                "county_state": df_pandas_data_frame_1.iloc[i, 2],
                "ssa_code" : df_pandas_data_frame_1.iloc[i, 3],
                "county_designation" : df_pandas_data_frame_1.iloc[i, 4],
                'specialty_code': specialty_code,
                'specialty': specialty,
                'time': time,
                'distance': distance
        })

# Convert the records list into a DataFrame
results_df = pd.DataFrame(records)

spark = SparkSession.builder \
    .appName("HSD File Processing") \
    .getOrCreate()

# Convert the Pandas DataFrame to Spark DataFrame
df_time_and_distance = spark.createDataFrame(results_df)

# Define the JDBC URL using the variables
jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

# Define connection properties using the variables
connection_properties = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver"
}

# Save the Spark DataFrame to PostgreSQL using the JDBC connection
df_time_and_distance.write \
    .jdbc(url=jdbc_url, table="time_and_distance", mode="overwrite", properties=connection_properties)


spark.stop()
