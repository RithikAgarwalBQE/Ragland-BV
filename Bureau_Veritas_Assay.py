import pandas as pd
import time
import warnings
import os
import re
import numpy as np
import logging
from util import Extract
warnings.filterwarnings("ignore")


def clean_result(value):
    if isinstance(value, str):
        if re.match(r'^<\d+(\.\d+)?$', value) or re.match(r'^\d+(\.\d+)?$', value):
            return value
    return np.nan


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
    
    
    try:
        #Reading the data and saving it in a dataframe, the ecoding is mentioned here because the csv file from the lab is not general encoding
        df = pd.read_excel(file_path)
    except FileNotFoundError:
        print(f"Could not find the file {file}")
        continue
    except pd.errors.ParserError:
        print(f"Error reading the file {file}. There might be a problem with its contents.")
        continue
    except Exception as e:
        print(f"An unexpected error occurred while reading the file {file}: {str(e)}")
        continue


    try:
        Sample_no = list(df['Sample #'].unique())
        Sample_no = df[['Sample #', 'Client Sample #']].drop_duplicates().values.tolist()

        project_no = df['Project #'].loc[0]
        job_no = df['Job #'].loc[0]

        df['Result'] = df['Result'].apply(clean_result)

        metal_df = df[
            df['Test'].str.contains('Total', case=False, na=False)
        ]
        metal_df['Parameter'] = metal_df['Parameter'].str.replace('Total Extractable ', '')

        metal_final_df = metal_df[['Sampling Date','Client Sample #', 'Test', 'Parameter', 'Result']]

        df_pivot_metal = metal_final_df.pivot_table(index=['Sampling Date', 'Client Sample #'], columns='Parameter', values='Result', aggfunc=lambda x: ' '.join(str(v) for v in x)).reset_index()

        df_sample_no = pd.DataFrame(Sample_no, columns=['Sample #', 'Client Sample #'])

        df_pivot_metal = df_pivot_metal.merge(df_sample_no, left_on='Client Sample #', right_on='Client Sample #', how='left')
        # df_pivot_metal['Sample #'] = Sample_no
        df_pivot_metal['Project #'] = project_no
        df_pivot_metal['Job #'] = job_no

        df_pivot_metal.insert(1, 'Sample #', df_pivot_metal.pop('Sample #'))
        df_pivot_metal.insert(2, 'Project #', df_pivot_metal.pop('Project #'))
        df_pivot_metal.insert(3, 'Job #', df_pivot_metal.pop('Job #'))

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


    # Writing_file = '../data/MasterData.xlsx'
    Writing_file = 'data\MasterData.xlsx'

    try:
        # reading data from the excle sheet named 'DATABASE' and Taking the data currently in the file in a dataframe
        metal_master_df = pd.read_excel(Writing_file, sheet_name = 'Metals')
    except pd.errors.EmptyDataError:
        metal_master_df = pd.DataFrame()
    except FileNotFoundError:
        print(f"File {Writing_file} does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")
        continue

    for idx, row in metal_master_df.iterrows():
        df_pivot_metal = df_pivot_metal[~((df_pivot_metal['Sample #'] == row['Sample #']) & (df_pivot_metal['Client Sample #'] == row['Client Sample #']))]

    df_appended_metal = metal_master_df._append(df_pivot_metal, ignore_index=True)

    df_appended_metal['Sampling Date'] = pd.to_datetime(df_appended_metal['Sampling Date'], errors='coerce')

    df_appended_metal.sort_values(by='Sampling Date', inplace =True)
    while True:
        try:
            with pd.ExcelWriter(Writing_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer: 
                df_appended_metal.to_excel(writer, sheet_name='Metals', index=False)
                print("Completed entry for Metals" )
                break
        except PermissionError:
            print("Unable to write to file. It may be open in another program.")
            input("Please close the file if it's open and press any key to try again.")
            # Optional: add a delay before next attempt to avoid rapid, successive attempts
            time.sleep(2)


    try:
        dissolved_metal_df = df[
            df['Test'].str.contains('Dissolved', case=False, na=False)
        ]
        no_dissolved_metal = False
        if dissolved_metal_df.empty:
            print("No dissolved metal metal")
            no_dissolved_metal = True
        else:
            dissolved_metal_df['Parameter'] = dissolved_metal_df['Parameter'].str.replace('Dissolved ', '')

            dissolved_metal_final_df = dissolved_metal_df[['Sampling Date','Client Sample #', 'Test', 'Parameter', 'Result']]

            df_pivot_metal_dissolved = dissolved_metal_final_df.pivot_table(index=['Sampling Date', 'Client Sample #'], columns='Parameter', values='Result', aggfunc=lambda x: ' '.join(str(v) for v in x)).reset_index()
            
            df_sample_no = pd.DataFrame(Sample_no, columns=['Sample #', 'Client Sample #'])

            df_pivot_metal_dissolved = df_pivot_metal_dissolved.merge(df_sample_no, left_on='Client Sample #', right_on='Client Sample #', how='left')
            
            #df_pivot_metal_dissolved['Sample #'] = Sample_no
            df_pivot_metal_dissolved['Project #'] = project_no
            df_pivot_metal_dissolved['Job #'] = job_no

            df_pivot_metal_dissolved.insert(1, 'Sample #', df_pivot_metal_dissolved.pop('Sample #'))
            df_pivot_metal_dissolved.insert(2, 'Project #', df_pivot_metal_dissolved.pop('Project #'))
            df_pivot_metal_dissolved.insert(3, 'Job #', df_pivot_metal_dissolved.pop('Job #'))

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
        try:
            # reading data from the excle sheet named 'DATABASE' and Taking the data currently in the file in a dataframe
            dissolved_metal_master_df = pd.read_excel(Writing_file, sheet_name = 'Dissolved')
        except pd.errors.EmptyDataError:
            dissolved_metal_master_df = pd.DataFrame()
        except FileNotFoundError:
            print(f"File {Writing_file} does not exist.")
        except Exception as e:
            print(f"An error occurred: {e}")
            continue       
        for idx, row in dissolved_metal_master_df.iterrows():
            df_pivot_metal_dissolved = df_pivot_metal_dissolved[~((df_pivot_metal_dissolved['Sample #'] == row['Sample #']) & (df_pivot_metal_dissolved['Client Sample #'] == row['Client Sample #']))]

        df_appended_metal_dissolved = dissolved_metal_master_df._append(df_pivot_metal_dissolved, ignore_index=True)

        df_appended_metal_dissolved['Sampling Date'] = pd.to_datetime(df_appended_metal_dissolved['Sampling Date'], errors='coerce')
        
        df_appended_metal_dissolved.sort_values(by='Sampling Date', inplace =True)
        while True:
            try:
                with pd.ExcelWriter(Writing_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer: 
                    df_appended_metal_dissolved.to_excel(writer, sheet_name='Dissolved', index=False)
                    print("Completed entry for Dissolved" )
                    break
            except PermissionError:
                print("Unable to write to file. It may be open in another program.")
                input("Please close the file if it's open and press any key to try again.")
                # Optional: add a delay before next attempt to avoid rapid, successive attempts
                time.sleep(2)


    try:
        Conventional_df = df[
            ~(df['Test'].str.contains('Dissolved|Total|Mercury', case=False, na=False))
        ]
        Conventional_final_df = Conventional_df[['Sampling Date','Client Sample #', 'Test', 'Parameter', 'Result']]

        df_pivot_conventional = Conventional_final_df.pivot_table(index=['Sampling Date', 'Client Sample #'], columns='Parameter', values='Result', aggfunc=lambda x: ' '.join(str(v) for v in x)).reset_index()
        
        df_sample_no = pd.DataFrame(Sample_no, columns=['Sample #', 'Client Sample #'])

        df_pivot_conventional = df_pivot_conventional.merge(df_sample_no, left_on='Client Sample #', right_on='Client Sample #', how='left')
        
        #df_pivot_conventional['Sample #'] = Sample_no
        df_pivot_conventional['Project #'] = project_no
        df_pivot_conventional['Job #'] = job_no


        df_pivot_conventional.insert(1, 'Sample #', df_pivot_conventional.pop('Sample #'))
        df_pivot_conventional.insert(2, 'Project #', df_pivot_conventional.pop('Project #'))
        df_pivot_conventional.insert(3, 'Job #', df_pivot_conventional.pop('Job #'))

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

    try:
        # reading data from the excle sheet named 'DATABASE' and Taking the data currently in the file in a dataframe
        conventional_master_df = pd.read_excel(Writing_file, sheet_name = 'Conventional')
    except pd.errors.EmptyDataError:
        conventional_master_df = pd.DataFrame()
    except FileNotFoundError:
        print(f"File {Writing_file} does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")
        input("Press Enter to end ")
        continue
    for idx, row in conventional_master_df.iterrows():
        df_pivot_conventional = df_pivot_conventional[~((df_pivot_conventional['Sample #'] == row['Sample #']) & (df_pivot_conventional['Client Sample #'] == row['Client Sample #']))]

    df_appended_conventional = conventional_master_df._append(df_pivot_conventional, ignore_index=True)

    df_appended_conventional['Sampling Date'] = pd.to_datetime(df_appended_conventional['Sampling Date'], errors='coerce')
    
    df_appended_conventional.sort_values(by='Sampling Date', inplace =True)
    while True:
        try:
            with pd.ExcelWriter(Writing_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer: 
                df_appended_conventional.to_excel(writer, sheet_name='Conventional', index=False)
                print("Completed entry for Conventional" )
                break
        except PermissionError:
            print("Unable to write to file. It may be open in another program.")
            input("Please close the file if it's open and press any key to try again.")
            # Optional: add a delay before next attempt to avoid rapid, successive attempts
            time.sleep(2)

    print(f"completed entry for file {file}")
    print("...................................")

for file in file_list:
    file_path = 'files\\' + file
    # file_path = '../files/' + file
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"File {file_path} has been deleted.")
    else:
        print(f"File {file_path} does not exist.")

input("Master Data Updated, please press Enter to end")