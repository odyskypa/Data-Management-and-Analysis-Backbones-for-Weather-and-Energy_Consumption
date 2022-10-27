# Moving data sources from the temporal zone to the persistent zone
### This script automatically copies all datasources to persistent zone and adds timestamp to them
#### Data are uploaded manually in the temporal zone
#In the temporal zone everything is created manually. For each data source there is a folder containing the different versions of the data. 
# When new versions of the different data sources are available online, the data manager should download and copy them in the appropriate folder. 
# Then the scripts accomplish all the work automatically.

# Importing the appropriate libraries and setting the paths needed for the script"""

import os
import shutil
from datetime import datetime
from paths import temporalPath,persistentPath

# In the operation stage this need to be a different file containing important
# paths, which will be loaded by the code automatically


"""# Definition of functions for the script"""

def getBaseAndExtensionOfFile(file):
    """
      Handles base and extension separation for files of different type, like xlsx, tar.gz, etc.
      In order to add more file types, just upload the list "types_of_files".

      @param
        -  file: the absolute path of a file
      @Output:
        - base: the base string of the file's name
        - extension: the extension string of the file's name
    """

    # A list of types of files
    types_of_files = [".xlsx", ".csv", ".tar.gz"]
    # A list of folder separation characters
    folder_separation_characters = ["//", "\\", "/"]

    # Checking which is the type of separation character used in the absolute path of the file
    for folder_separation_character in folder_separation_characters:
      if folder_separation_character in file:
        separtion_character = folder_separation_character

    # Take only the filename from the absolute path of file
    filename = file.split(separtion_character)[-1]

    # Check the type of file from the list of types and separate base and extension
    for file_type in types_of_files:
      if filename.endswith(file_type):
        base = filename.split(file_type)[0]
        extension = file_type
        break

    # Return the base and extension of the filename
    return base, extension

def main():
    
    if not os.path.exists(persistentPath):
        os.mkdir(persistentPath)

    # Getting the current date and time
    dt = datetime.now()

    # getting the timestamp
    ts = datetime.timestamp(dt)

    # Walk through all files in the directory that contains the files to copy

    # Variable dirs contain a list with all the folders in the temporal zone
    for _, dirs, _ in os.walk(temporalPath):
    # Loop through each folder in the temporal zone
        for dir in dirs:
            # Variable root contain the path for each folder in the temporal zone
            # Variable files contain a list with all the files inside each folder
            for root, _, files in os.walk(temporalPath + dir):
            # Loop through each file of the folder
                for file in files:
                    # Skipping .ini files
                    if not file.endswith('ini'):

                    # I use absolute path, case you want to move several dirs.
                    # Variable old_name containing the absolute path of the data source from termporal zone.
                        old_name = os.path.join(os.path.abspath(root), file)
                        
                        # Separate base from extension
                        # Created getBaseAndExtensionOfFile function in order to deal with different types of files
                        base, extension = getBaseAndExtensionOfFile(old_name)

                        # Variable notime contains only the part of the timestamp before the dot
                        notime = str(ts).split('.')[0]

                        # Variable new_folder_name contain the absolute path for the new folder with timestamp in the persistent zone
                        new_folder_name = persistentPath +  dir + "_" + notime + "/"
                        # Variable new_final_name contain the absolute path for the new file with timestamp in the persistent zone
                        new_final_name = new_folder_name + f"{base}_{notime}{extension}"
                        # Checking if new_folder_name exists, otherwise it is being created here
                        if not os.path.exists(new_folder_name):
                            os.mkdir(new_folder_name)
                        # Copying the file from temporal zone into the persistent zone with timestamp
                        shutil.copy(old_name, new_final_name)


if __name__ == "__main__":
    main()

