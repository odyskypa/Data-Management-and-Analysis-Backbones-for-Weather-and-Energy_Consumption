import pandas as pd
from sklearn.model_selection import train_test_split
from utilities.db_utilities import getDataframeFrom_exploitation, saveDataframeTo_exploitation

def main():
    df = getDataframeFrom_exploitation("Analysis")
    len(df.columns)
    X = df.iloc[:, :-1]
    y = df.iloc[:, -1]
    X_train, X_test, y_train, y_test = train_test_split(
                                        X,
                                        y,
                                        train_size   = 0.8,
                                        random_state = 1234,
                                        shuffle      = True)
    df_train = pd.concat([X_train, y_train], axis=1) #Merging explanatory variables and target variable for training test after splitting
    df_test = pd.concat([X_test, y_test], axis=1) #Merging explanatory variables and target variable for validation test after splitting
    saveDataframeTo_exploitation(df_train, "training_set")
    saveDataframeTo_exploitation(df_test, "validation_set")
    print("Training and Validation sets have been saved in exploitation database with names: training_set and validation_set respectively")

if __name__ == "__main__":
    main()