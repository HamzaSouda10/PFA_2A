import pandas as pd
from sqlalchemy import create_engine, String, Integer

def etl_fact_inventory(source_conn_str, target_conn_str):
    try:
        source_engine = create_engine(source_conn_str)
        query = """
            SELECT 
                pi.ProductID,
                CONVERT(INT, FORMAT(pih.TransactionDate, 'yyyyMMdd')) AS DateID,
                pi.LocationID,
                pi.Quantity
            FROM Production.ProductInventory pi
            JOIN Production.TransactionHistory pih ON pi.ProductID = pih.ProductID
        """
        df = pd.read_sql(query, source_engine)
        print(f"Extraction FactInventory réussie : {len(df)} lignes")

    except Exception as e:
        print(f"Erreur d'extraction FactInventory : {e}")
        return

    try:
        target_engine = create_engine(target_conn_str)
        dtype_mapping = {
            'ProductID': Integer,
            'DateID': Integer,
            'LocationID': Integer,
            'Quantity': Integer,
        }

        df.to_sql('FactInventory', target_engine, if_exists='replace', index=False, dtype=dtype_mapping)
        print("Chargement FactInventory réussi")

    except Exception as e:
        print(f"Erreur de chargement FactInventory : {e}")
