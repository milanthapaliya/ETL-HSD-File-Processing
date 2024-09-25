import pandas as pd
import gc

def extract_time_and_distance(file_path):
    # Load the Excel sheet directly using Pandas
    df_pandas = pd.read_excel(file_path, sheet_name="Provider Time & Distance", header=None)

    # Extract data without headings
    df_pandas_data_frame_1 = df_pandas.drop(index=[0, 1, 2, 3])
    df_pandas_data_frame_2 = df_pandas.drop(index=[0, 1, 2, 3])

    # Get headings
    headings_frame_1 = df_pandas.loc[[1]]
    headings_frame_2 = df_pandas.loc[1:3]

    # Reset the index after dropping the rows
    df_pandas_data_frame_1.reset_index(drop=True, inplace=True)
    df_pandas_data_frame_2.reset_index(drop=True, inplace=True)
    headings_frame_1.reset_index(drop=True, inplace=True)
    headings_frame_2.reset_index(drop=True, inplace=True)

    # Select only the first 5 columns
    df_pandas_data_frame_1 = df_pandas_data_frame_1.iloc[:, :5]
    headings_frame_1 = headings_frame_1.iloc[:, :5]
    # Select only after first 5 columns
    df_pandas_data_frame_2 = df_pandas_data_frame_2.iloc[:, 5:]
    headings_frame_2 = headings_frame_2.iloc[:, 5:]

    records = []

    # Logic to extract time and distance for each row in dataframe_1
    for i in range(len(df_pandas_data_frame_1)):
        for j in range(df_pandas_data_frame_2.shape[1]):
            if j % 2 == 0:
                specialty = headings_frame_2.iloc[0, j]  # Specialty name is in row 0
                specialty_code = headings_frame_2.iloc[1, j]  # Specialty code is in row 1

                if j + 1 < df_pandas_data_frame_2.shape[1]:
                    time = df_pandas_data_frame_2.iloc[i, j]  # Assuming time is in the current column
                    distance = df_pandas_data_frame_2.iloc[i, j + 1]  # Distance in the next column

                records.append({
                    'county': df_pandas_data_frame_1.iloc[i, 0],
                    'state': df_pandas_data_frame_1.iloc[i, 1],
                    "county_state": df_pandas_data_frame_1.iloc[i, 2],
                    "ssa_code": df_pandas_data_frame_1.iloc[i, 3],
                    "county_designation": df_pandas_data_frame_1.iloc[i, 4],
                    'specialty_code': specialty_code,
                    'specialty': specialty,
                    'time': time,
                    'distance': distance
                })

    # Convert the records list into a DataFrame
    results_df = pd.DataFrame(records)
    # Delete intermediate DataFrames to free up memory
    del df_pandas, df_pandas_data_frame_1, df_pandas_data_frame_2, headings_frame_1, headings_frame_2
    gc.collect()  # Optionally call garbage collector
    return results_df
