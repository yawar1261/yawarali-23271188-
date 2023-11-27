import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine


def Extract_Data(url):
    print("data extraction from the url")
    try:
        original_df = pd.read_csv(url, sep=';', low_memory=False)
        #print(original_df)
        return original_df
    except Exception as ex:
        print("reading failed because ", str(ex))
        return None

    print("Extraction completed successfully ")


def Transform_Data(Data_Train_CSV):
    print("Data Transformation ")
    try:
        # Drop Status column
        Data_Train_CSV = Data_Train_CSV.drop('Status', axis=1)

        # Drop invalid values rows
        Data_Train_CSV['Laenge'] = Data_Train_CSV['Laenge'].str.replace(',', '.').astype(float)
        Data_Train_CSV['Breite'] = Data_Train_CSV['Breite'].str.replace(',', '.').astype(float)

        # Validate "Verkehr", "Laenge", "Breite", "IFOPT" values
        # Drop empty cells

        Data_Train_CSV = Data_Train_CSV[
            (Data_Train_CSV["Verkehr"].isin(["FV", "RV", "nur DPN"])) &
            (Data_Train_CSV["Laenge"].between(-90, 90)) &
            (Data_Train_CSV["Breite"].between(-90, 90)) &
            (Data_Train_CSV["IFOPT"].str.match(r"^[A-Za-z]{2}:\d+:\d+(?::\d+)?$"))
            ].dropna()

        # Change data type
        data_type = {
            "EVA_NR": int,
            "DS100": str,
            "IFOPT": str,
            "NAME": str,
            "Verkehr": str,
            "Laenge": float,
            "Breite": float,
            "Betreiber_Name": str,
            "Betreiber_Nr": int
        }
        # Store changed data
        Data_Train_Transformed = Data_Train_CSV.astype(data_type)
    except Exception as ex:
        print("Error in data transformation: ", str(ex))
        return None

    print("Transformation completed ")
    return Data_Train_Transformed


def LoadData(Data_Train_Transformed, table_name):
    print("SQLite DB from transformed data ")
    try:
        engine = create_engine(f"sqlite:///trainstops.sqlite")
        Data_Train_Transformed.to_sql(table_name, engine, if_exists="replace", index=False)
        print("Database created successfully ")
    except Exception as ex:
        print("Error: ", str(ex))


def output():
    url = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
    Data_Train_CSV = Extract_Data(url)
    if Data_Train_CSV is not None:
        Data_Train_Transformed = Transform_Data(Data_Train_CSV)
        if Data_Train_Transformed is not None:
            LoadData(Data_Train_Transformed, "trainstops")
        else:
            print("transformation failed")
    else:
        print("extraction failed")


if __name__ == "__main__":
    output()
