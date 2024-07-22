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