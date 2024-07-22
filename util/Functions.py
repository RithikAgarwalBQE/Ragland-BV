import os
import pandas as pd
from . import Lists
import warnings
import time
import numpy as np
import re
import logging

def clean_result(value):
    if isinstance(value, str):
        if re.match(r'^<\d+(\.\d+)?$', value) or re.match(r'^\d+(\.\d+)?$', value):
            return value
    return np.nan

def write_data(Writing_file, df, sheet):

    while True:
        try:
            with pd.ExcelWriter(Writing_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer: 
                df.to_excel(writer, sheet_name= sheet, index=False)
                print("Completed entry for Metals" )
                break
        except PermissionError:
            print("Unable to write to file. It may be open in another program.")
            input("Please close the file if it's open and press any key to try again.")
            # Optional: add a delay before next attempt to avoid rapid, successive attempts
            time.sleep(2)
        except Exception as e:
            print(f"An error occurred: {e}")
            input('Press any key to enc')
            exit(1)

def transform_parameters(df, test_type, parameter):
    
    Sample_no = list(df['Sample #'].unique())
    Sample_no = df[['Sample #', 'Client Sample #']].drop_duplicates().values.tolist()

    project_no = df['Project #'].loc[0]
    job_no = df['Job #'].loc[0] 
    
    df = df[
        df['Test'].str.contains(test_type, case=False, na=False)
    ]

    parameter_check = False

    if df.empty:
        parameter_check = True
        df_pivot = pd.DataFrame()

        return df_pivot, parameter_check
    else:

        if test_type != 'Dissolved|Total|Mercury':
            df['Parameter'] = df['Parameter'].str.replace(parameter, '')

        final_df = df[['Sampling Date','Client Sample #', 'Test', 'Parameter', 'Result']]

        df_pivot = final_df.pivot_table(index=['Sampling Date', 'Client Sample #'], columns='Parameter', values='Result', aggfunc=lambda x: ' '.join(str(v) for v in x)).reset_index()
        
        df_sample_no = pd.DataFrame(Sample_no, columns=['Sample #', 'Client Sample #'])

        df_pivot = df_pivot.merge(df_sample_no, left_on='Client Sample #', right_on='Client Sample #', how='left')
        df_pivot['Project #'] = project_no
        df_pivot['Job #'] = job_no

        df_pivot.insert(1, 'Sample #', df_pivot.pop('Sample #'))
        df_pivot.insert(2, 'Project #', df_pivot.pop('Project #'))
        df_pivot.insert(3, 'Job #', df_pivot.pop('Job #'))

    return df_pivot, parameter_check
        