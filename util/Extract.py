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