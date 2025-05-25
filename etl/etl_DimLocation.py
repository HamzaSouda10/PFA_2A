import pandas as pd
from sqlalchemy import create_engine, String, Integer

def etl_dim_location(source_conn_str, target_conn_str):
    try:
        source_engine = create_engine(source_conn_str)
        query = """
            SELECT 
                l.LocationID,
                l.Name AS LocationName,
                a.AddressLine1,
                a.AddressLine2,
                a.City,
                sp.Name AS StateProvince,
                a.PostalCode,
                cr.Name AS CountryRegion
            FROM Production.Location l
            JOIN Person.BusinessEntityAddress bea ON l.LocationID = bea.AddressID
            JOIN Person.Address a ON bea.AddressID = a.AddressID
            JOIN Person.StateProvince sp ON a.StateProvinceID = sp.StateProvinceID
            JOIN Person.CountryRegion cr ON sp.CountryRegionCode = cr.CountryRegionCode
        """
        df = pd.read_sql(query, source_engine)
        print(f"Extraction DimLocation réussie : {len(df)} lignes")

    except Exception as e:
        print(f"Erreur d'extraction DimLocation : {e}")
        return

    try:
        target_engine = create_engine(target_conn_str)
        dtype_mapping = {
            'LocationID': Integer,
            'LocationName': String(50),
            'AddressLine1': String(100),
            'AddressLine2': String(100),
            'City': String(50),
            'StateProvince': String(50),
            'PostalCode': String(15),
            'CountryRegion': String(50),
        }

        df.to_sql('DimLocation', target_engine, if_exists='replace', index=False, dtype=dtype_mapping)
        print("Chargement DimLocation réussi")

    except Exception as e:
        print(f"Erreur de chargement DimLocation : {e}")
