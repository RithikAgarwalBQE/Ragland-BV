import pandas as pd
import time
import warnings
import os
import re
import numpy as np
import logging
from util import Extract
from util import Functions
from util import Post_Processing
warnings.filterwarnings("ignore")


logging.basicConfig(
    level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='Logs.log'  # Output log messages to a file
)

#Production = 0 when application is under build and production = 1 when the application is deployed
production = 0

#setting up path to all the required files
if production == 0:
    folder_path = 'Input_files'
    file_path = folder_path + '\\'
    Writing_file = 'Database\\' + 'MasterData.xlsx'
    destination = 'Archive_Files'
else:
    folder_path = "../Input_files"
    file_path = folder_path + '/'
    Writing_file = '../Database/' + 'MasterData.xlsx'
    destination = "../Archive_Files"


file_list = Extract.get_file_list(folder_path)


print("Starting...............")

error_files = []

for file in file_list:

    print("Working on file", file)
    file_path = folder_path + file
    

    df = Extract.read_file(file_path, file)

    if df.empty:
        continue

    try:
        df['Result'] = df['Result'].apply(Functions.clean_result())

        df_pivot_metal, no_metal  = Functions.transform_parameters(df, test_type='Total', parameter ='Total Extractable ' )
        
        if df_pivot_metal.empty:
            print("No total metal")
            no_metal = True
        
        else:
            list_of_columns = df_pivot_metal.columns.tolist()

            list_of_columns = list_of_columns[5:]

            for i in list_of_columns:
                df_pivot_metal[i] = df_pivot_metal[i].apply(lambda x: float(x) if not x.startswith('<') else x)
    except KeyError as e:
        print(f"A KeyError occurred: {str(e)}. Please make sure the necessary columns exist in the input data.")
        continue
    except Exception as e:
        print(f"An error occurred while processing the data: {str(e)}")
        continue


    metal_master_df = Extract.read_database(Writing_file, sheet = 'Metals')

    for idx, row in metal_master_df.iterrows():
        df_pivot_metal = df_pivot_metal[~((df_pivot_metal['Sample #'] == row['Sample #']) & (df_pivot_metal['Client Sample #'] == row['Client Sample #']))]

    df_appended_metal = metal_master_df._append(df_pivot_metal, ignore_index=True)

    df_appended_metal['Sampling Date'] = pd.to_datetime(df_appended_metal['Sampling Date'], errors='coerce')

    df_appended_metal.sort_values(by='Sampling Date', inplace =True)

        
    Functions.write_data(Writing_file, df_appended_metal, sheet='Metals')


    try:
        df_pivot_metal_dissolved,no_dissolved_metal  = Functions.transform_parameters(df, test_type='Dissolved', parameter= 'Dissolved ')


        if df_pivot_metal_dissolved.empty:
            print("No dissolved metal")
            no_dissolved_metal = True
        else:

            list_of_columns = df_pivot_metal_dissolved.columns.tolist()

            list_of_columns = list_of_columns[5:]

            for i in list_of_columns:
                df_pivot_metal_dissolved[i] = df_pivot_metal_dissolved[i].apply(lambda x: float(x) if not x.startswith('<') else x)
    except KeyError as e:
        print(f"A KeyError occurred: {str(e)}. Please make sure the necessary columns exist in the input data.")
        continue
    except Exception as e:
        print(f"An error occurred while processing the data: {str(e)}")
        continue

    if no_dissolved_metal == False:      

        dissolved_metal_master_df = Extract.read_database(Writing_file, sheet= 'Dissolved')

        for idx, row in dissolved_metal_master_df.iterrows():
            df_pivot_metal_dissolved = df_pivot_metal_dissolved[~((df_pivot_metal_dissolved['Sample #'] == row['Sample #']) & (df_pivot_metal_dissolved['Client Sample #'] == row['Client Sample #']))]

        df_appended_metal_dissolved = dissolved_metal_master_df._append(df_pivot_metal_dissolved, ignore_index=True)

        df_appended_metal_dissolved['Sampling Date'] = pd.to_datetime(df_appended_metal_dissolved['Sampling Date'], errors='coerce')
        
        df_appended_metal_dissolved.sort_values(by='Sampling Date', inplace =True)

        Functions.write_data(Writing_file, df_appended_metal_dissolved, sheet='Dissolved')

    try:
        
        df_pivot_conventional,no_conventional  = Functions.transform_parameters(df, test_type='Dissolved|Total|Mercury', parameter= 'Dissolved ')

        if df_pivot_conventional.empty:
            print("No dissolved metal")
            no_conventional = True
        else:
            list_of_columns = df_pivot_conventional.columns.tolist()

            list_of_columns = list_of_columns[5:]

            for i in list_of_columns:
                df_pivot_conventional[i] = df_pivot_conventional[i].apply(lambda x: float(x) if not x.startswith('<') else x)

    except KeyError as e:
        print(f"A KeyError occurred: {str(e)}. Please make sure the necessary columns exist in the input data.")
        input("Press Enter to end ")
        continue
    except Exception as e:
        print(f"An error occurred while processing the data: {str(e)}")
        input("Press Enter to end ")
        continue
    
    conventional_master_df = Extract.read_database(Writing_file, sheet='Conventional')

    for idx, row in conventional_master_df.iterrows():
        df_pivot_conventional = df_pivot_conventional[~((df_pivot_conventional['Sample #'] == row['Sample #']) & (df_pivot_conventional['Client Sample #'] == row['Client Sample #']))]

    df_appended_conventional = conventional_master_df._append(df_pivot_conventional, ignore_index=True)

    df_appended_conventional['Sampling Date'] = pd.to_datetime(df_appended_conventional['Sampling Date'], errors='coerce')
    
    df_appended_conventional.sort_values(by='Sampling Date', inplace =True)

    Functions.write_data(Writing_file, df_appended_conventional, sheet='Conventional' )

    print(f"completed entry for file {file}")
    print("...................................")

Post_Processing.remove_files(file_list, folder_path)

input("Master Data Updated, please press Enter to end")