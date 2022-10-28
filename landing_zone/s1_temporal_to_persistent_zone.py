from utilities.os_utilities import createDirectory, getTimestamp, copyFilesOfSourceDirToDestDirWithTimestamp
from paths import temporalPath, persistentPath

# Moving data sources from the temporal zone to the persistent zone.
# This script automatically copies all datasources to persistent zone and adds timestamp to them.
# Data are uploaded manually in the temporal zone.
# In the temporal zone everything is created manually. For each data source there is a folder containing the different versions of the data. 
# When new versions of the different data sources are available online, the data manager should download and copy them in the appropriate folder. 
# Then the scripts accomplish all the work automatically.

def main():

    createDirectory(persistentPath)
    ts = getTimestamp()
    copyFilesOfSourceDirToDestDirWithTimestamp(temporalPath, persistentPath, ts)

if __name__ == "__main__":
    main()

