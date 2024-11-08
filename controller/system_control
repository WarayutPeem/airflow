import os, shutil

# clean file
def clean_file(path_file):
    if os.path.exists(path_file):
        os.remove(path_file)
        print(f"File {path_file} deleted locally.")
    else:
        print(f"The file {path_file} does not exist.")


# clean folder and file
def clean_folder(path_folder):
    if os.path.exists(path_folder) and os.path.isdir(path_folder):
        shutil.rmtree(path_folder)


# create folder for save all file
def if_exists_folder(path_full):
    if path_full:
        if not os.path.exists(path_full):
            os.makedirs(path_full, exist_ok=True)
            os.chmod(path_full, 0o777)

    return path_full