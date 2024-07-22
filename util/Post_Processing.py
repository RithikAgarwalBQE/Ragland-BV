import os

def remove_files(file_list, folder_path):
    for file in file_list:
        file_path = folder_path + file
        # file_path = '../files/' + file
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"File {file} has been deleted.")
        else:
            print(f"File {file} does not exist.")