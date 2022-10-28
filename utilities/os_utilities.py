import os
import shutil
from datetime import datetime

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

def getDataSourcesNames(temporalPath):
    """
    Getting a list with all the names of the data sources saved in the landing zone inside the temporal folder.
    Inside temporal folder there is one folder for landing each data source and their versions.

    @param:
        -   temporalPath: the absolute path of the temporal folder
    @Output:
        -   data_sources_names: a list with all the names of the different datasources
    """
    for _, dirs, _ in os.walk(temporalPath):
        if len(dirs) > 0:
            data_sources_names = dirs
    return data_sources_names

def createDirectory(directoryPath):
    """
    Creating the directory provided as input.

    @param:
        -   directoryPath: the absolute path of directory to create
    @Output:
    """
    if not os.path.exists(directoryPath):
        os.mkdir(directoryPath)

def getTimestamp():
    """
        Generating and returning a timestamp

        @param:
        @Output:
            -   ts: the timestamp to be returned
    """

    # Getting the current date and time
    dt = datetime.now()

    # getting the timestamp
    ts = datetime.timestamp(dt)
    return ts

def copyFilesOfSourceDirToDestDirWithTimestamp(sourceDir, destDir, timestamp):
    """
    Copying all directories and files inside the source folder to the destination folder.
    Additionally, timestamps are added to the names of all directories and files to the destination folder.

    @param:
        -   sourceDir: the absolute path of the source directory
        -   destDir: the absolute path of the destination directory
        -   timestamp: the timestamp which is added to all the names of the files and directories

    @Output:
    """
    
    # Walk through all files in the directory that contains the files to copy
    # Variable dirs contain a list with all the directories in the temporal zone
    for _, dirs, _ in os.walk(sourceDir):
    # Loop through each folder in the temporal zone
        for dir in dirs:
            # Variable root contain the path for each folder in the temporal zone
            # Variable files contain a list with all the files inside each folder
            for root, _, files in os.walk(sourceDir + dir):
            # Loop through each file of the folder
                for file in files:
                    # Skipping .ini files
                    if not file.endswith('ini'):

                        # Variable old_name containing the absolute path of the data source from termporal zone.
                        old_name = os.path.join(os.path.abspath(root), file)
                        
                        # Separate base from extension
                        base, extension = getBaseAndExtensionOfFile(old_name)

                        # Variable time contains only the part of the timestamp before the dot
                        time = str(timestamp).split('.')[0]

                        # Variable new_folder_name contain the absolute path for the new folder with timestamp in the persistent zone
                        new_folder_name = destDir +  dir + "_" + time + "/"

                        # Variable new_final_name contain the absolute path for the new file with timestamp in the persistent zone
                        new_final_name = new_folder_name + f"{base}_{time}{extension}"
                        
                        createDirectory(new_folder_name)
                        
                        # Copying the file from temporal zone into the persistent zone with timestamp
                        print(f"\n Creating a copy of file: {old_name} to --> {new_final_name} \n")
                        shutil.copy(old_name, new_final_name)