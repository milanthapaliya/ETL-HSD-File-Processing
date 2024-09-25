import pandas as pd
import gc

def extract_minimums(file_path, time_distance_dataframe):
    # Load the Excel sheet directly using Pandas
    df_pandas = pd.read_excel(file_path, sheet_name="Minimum Provider #s", header=None)

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

    # Select only the first 8 columns
    df_pandas_data_frame_1 = df_pandas_data_frame_1.iloc[:, :8]
    headings_frame_1 = headings_frame_1.iloc[:, :8]
    # Select only after first 8 columns
    df_pandas_data_frame_2 = df_pandas_data_frame_2.iloc[:, 8:]
    headings_frame_2 = headings_frame_2.iloc[:, 8:]

    records = []

    # Logic to extract time and distance for each row in dataframe_1
    for i in range(len(df_pandas_data_frame_1)):
        for j in range(df_pandas_data_frame_2.shape[1]):
                specialty_code = headings_frame_2.iloc[1, j]  # Specialty code is in row 1
                provider_count = time = df_pandas_data_frame_2.iloc[i, j] # Assuming in the current column
                records.append({
                    "ssa_code": df_pandas_data_frame_1.iloc[i, 3],
                    'hsd_specialty_cd': specialty_code,
                    'min_provider_count': provider_count,
                })

    # Convert the records list into a DataFrame
    results_df = pd.DataFrame(records)
    #perform join on time_distance_dataframe
    time_distance_dataframe = time_distance_dataframe[['specialty_code', 'time','distance','ssa_code']]
    time_distance_dataframe = time_distance_dataframe.rename(columns={
        'time': 'max_provider_time',
        'distance': 'max_provider_distance'
    })
   # results_df = results_df.replace('',None).dropna(subset=['time','distance'], how = 'all')
    merged_df = pd.merge(results_df, time_distance_dataframe,
                     left_on=['hsd_specialty_cd', 'ssa_code'],
                     right_on=['specialty_code', 'ssa_code'],
                     how='inner')

    # Drop the 'specialty_code' column from the merged DataFrame
    merged_df = merged_df.drop(columns=['specialty_code'])
    return merged_df
