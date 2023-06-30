# ADSDB PROJECT
Project of Algorithms Data Structures and Databases (ADSDB) Course for Master in Data Science Program of Universitat Politècnica de Catalunya (UPC)
Master in Data Science
Universitat Politècnica de Catalunya - Barcelona Tech
Ander Barrio Campos and Odysseas Kyparissis
***

## Instuctions for running the software
1. Initiliazation
    - For installing all the required libraries to your local system before executing the software please run the following command:
        - pip install -r requirements.txt
    - For running the software for the **first time**, inside "landing/temporal" folder create a folder for each data source and put inside the data files.
    - For **uploading new data source versions** go to "landing/temporal" folder and inside the folder of the specific data source add the new data files.
    - Before moving on to step 2, make sure to change the paths in the "paths.py" file in the main folder of the project. Change only the paths where the indication "# CHANGE THIS" exists.
2. For execution of the software, you can run "main.py", which runs sequentally all the main scripts of the project in the right order.
    - The main scripts are named as follows: **s"X"_"name_of_the_script"**
        - where **"X"** is the order of execution and 
        - **"name_of_the_script"** is a small description of the content of the script.
