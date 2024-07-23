import os

def remove_files(file_list, file_path):
    for file in file_list:
        file_location = file_path + file
        # file_path = '../files/' + file
        if os.path.exists(file_location):
            os.remove(file_location)
            print(f"File {file} has been deleted.")
        else:
            print(f"File {file} does not exist.")