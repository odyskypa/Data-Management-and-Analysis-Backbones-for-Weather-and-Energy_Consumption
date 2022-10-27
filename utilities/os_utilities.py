import os

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
        -    temporalPath: the absolute path of the temporal folder
    @Output:
        - data_sources_names: a list with all the names of the different datasources
    """
    for _, dirs, _ in os.walk(temporalPath):
        if len(dirs) > 0:
            data_sources_names = dirs
    return data_sources_names

def createDirectory(directoryPath):
    # Checking if reports directory exists, if not it is being created
    if not os.path.exists(directoryPath):
        os.mkdir(directoryPath)