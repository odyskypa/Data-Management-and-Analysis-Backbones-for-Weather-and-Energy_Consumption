import pandas as pd
import numpy as np
from sklearn.impute import SimpleImputer
from utilities.os_utilities import getDataSourcesNames
from utilities.db_utilities import getDataframeFrom_trusted, saveDataframeTo_trusted_noNAs
from paths import temporalPath

def NAs_handling(df, data_source_name):
    """
      Imputing missing values into a dataframe. This function separates the dataframe into numerical and categorical variables
      and then imputes missing values by using SimpleImputer from sklearn library

      @param:
        -  df: the dataframe to be imputed
        - data_source_name: the name of the data source
      @Output:
        - df_imputed: the final imputed dataframe
    """

    count_NAs = df.isnull().sum()
    df_number_of_rows = df.shape[0]
    print(f"\nTotal count of NaN values at each column in the {data_source_name} dataframe, before imputation:\n\n", count_NAs)
    for key, value in count_NAs.items():
        column_percentage_of_missing_value = value / df_number_of_rows # Calculating the persentage of missing values in each column
        if column_percentage_of_missing_value > 0.5: # Droping columns with percentage of missing more than 50%
            df = df.drop(key, axis=1)
    
    # Seperating dataframe to numerical and categorical variables
    # Keeping the column names in both cases
    numeric_df = df.select_dtypes(include=[np.number])
    numerical_cols = numeric_df.columns.values.tolist()
    categorical_df = df.select_dtypes(exclude=[np.number])
    categorical_cols = categorical_df.columns.values.tolist()

    # Creating SimpleImputer objects
    numerical_mean_imputer = SimpleImputer(missing_values=np.nan, strategy='mean')
    categorical_most_freq_imputer = SimpleImputer(missing_values=np.nan, strategy='most_frequent')

    # Training SimpleImputer for numerical variables
    # Imputing missing values for numerical variables
    numeric_fitted_data = numerical_mean_imputer.fit(numeric_df)
    numeric_data = numeric_fitted_data.transform(numeric_df)
    numeric_df_imputed = pd.DataFrame(data = numeric_data, columns = numerical_cols)

    # Training SimpleImputer for categorical variables
    # Imputing missing values for categorical variables
    categorical_fitted_data = categorical_most_freq_imputer.fit(categorical_df)
    categorical_data = categorical_fitted_data.transform(categorical_df)
    categorical_df_imputed = pd.DataFrame(data = categorical_data, columns = categorical_cols)
    
    # Joining the numerical and categorical imputed dataframes into one
    df_imputed = pd.concat([categorical_df_imputed, numeric_df_imputed], axis=1)
    #print(df_imputed.head)
    
    return df_imputed


def main():
    """
    # Handling of NA values for every data source in the trusted database 
    ## For every data source handle their missing values. 
    """

    # Getting the names of the different data sources
    data_sources_names = getDataSourcesNames(temporalPath)

    # Creating profile for each data source, results saved in reports folder 
    for data_source_name in data_sources_names:
        
        print(f"\nHandling NAs for {data_source_name} data source")

        df = getDataframeFrom_trusted(data_source_name)
        if data_source_name == "NCEI": # NEEDS to move from here
            df['STATION'] = df.STATION.astype('category')
            df['FRSHTT'] = df.FRSHTT.astype('category')
        df = NAs_handling(df, data_source_name)
        print(df.head)
        saveDataframeTo_trusted_noNAs(df, data_source_name)


if __name__ == "__main__":
    main()