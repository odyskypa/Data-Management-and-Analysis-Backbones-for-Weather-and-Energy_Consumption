# Moving data sources from the temporal zone to the persistent zone
### This script automatically copies all datasources to persistent zone and adds timestamp to them
#### Data are uploaded manually in the temporal zone
# In the temporal zone everything is created manually. For each data source there is a folder containing the different versions of the data. 
# When new versions of the different data sources are available online, the data manager should download and copy them in the appropriate folder. 
# Then the scripts accomplish all the work automatically.

import os
import shutil
from datetime import datetime
from paths import temporalPath, persistentPath
from utilities.os_utilities import *

def main():
    
    createDirectory(persistentPath)

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

                        # Variable old_name containing the absolute path of the data source from termporal zone.
                        old_name = os.path.join(os.path.abspath(root), file)
                        
                        # Separate base from extension
                        base, extension = getBaseAndExtensionOfFile(old_name)

                        # Variable notime contains only the part of the timestamp before the dot
                        notime = str(ts).split('.')[0]

                        # Variable new_folder_name contain the absolute path for the new folder with timestamp in the persistent zone
                        new_folder_name = persistentPath +  dir + "_" + notime + "/"

                        # Variable new_final_name contain the absolute path for the new file with timestamp in the persistent zone
                        new_final_name = new_folder_name + f"{base}_{notime}{extension}"
                        
                        createDirectory(new_folder_name)
                        
                        # Copying the file from temporal zone into the persistent zone with timestamp
                        print(f"\n Creating a copy of file: {old_name} to --> {new_final_name} \n")
                        shutil.copy(old_name, new_final_name)


if __name__ == "__main__":
    main()

