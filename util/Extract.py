import logging
import pandas as pd
import os
from . import Functions

def get_file_list(folder_path):
    try:
        # Get a list of all files in the folder
        file_list = os.listdir(folder_path)
    except FileNotFoundError:
        print(f"Error: The folder '{folder_path}' does not exist.")
        file_list = []
    except PermissionError:
        print(f"Error: The program does not have permission to access the folder '{folder_path}'.")
        file_list = []
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        file_list = []
    
    return file_list

def read_file(file_path, file):
    try:
        #Reading the data and saving it in a dataframe, the ecoding is mentioned here because the csv file from the lab is not general encoding
        df = pd.DataFrame()
        df = pd.read_excel(file_path)
    except FileNotFoundError:
        print(f"Could not find the file {file_path}")
        return df
    except pd.errors.ParserError:
        print(f"Error reading the file {file}. There might be a problem with its contents.")
        return df
    except Exception as e:
        print(f"An unexpected error occurred while reading the file {file}: {str(e)}")
        return df
    
    return df

def read_database(Writing_file, sheet):

    try:
        # reading data from the excle sheet named 'DATABASE' and Taking the data currently in the file in a dataframe
        df = pd.read_excel(Writing_file, sheet_name = sheet)
    except pd.errors.EmptyDataError:
        df = pd.DataFrame()
    except FileNotFoundError:
        print(f"File {Writing_file} does not exist.")
        input('Press any key to enc')
        exit(1)
    except Exception as e:
        print(f"An error occurred: {e}")
        input('Press any key to enc')
        exit(1)
    return df