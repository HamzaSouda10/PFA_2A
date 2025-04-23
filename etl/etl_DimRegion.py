import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import create_engine, String, Integer

def etl_dim_region(source_conn_str, target_conn_str):
    try:
        # Establish source connection
        source_engine = create_engine(source_conn_str)
        
        # SQL query to extract region data
        query = """
            SELECT 
                [TerritoryID],
                [Name],
                [CountryRegionCode],
                [Group]
            FROM [AdventureWorks2022].[Sales].[SalesTerritory]
        """
        
        df_region = pd.read_sql(query, source_engine)
        print(f"Extraction successful: {len(df_region)} sales territories found")
        
    except Exception as e:
        print(f"Extraction error: {e}")
        return

    try:
        # Establish target connection
        target_engine = create_engine(target_conn_str)
        
        # Définition des types de données SQL Server explicites
        dtype_mapping = {
            'TerritoryID': Integer,  # Integer pour TerritoryID
            'Name': String(50),  # NVARCHAR(50) pour Name
            'CountryRegionCode': String(3),  # NVARCHAR(3) pour CountryRegionCode
            'Group': String(50),
        }
        
        # Load into DimRegion
        df_region.to_sql(
            'DimRegion',
            target_engine,
            if_exists='replace',
            index=False,
            dtype=dtype_mapping
        )
        print(f"Loading successful: {len(df_region)} rows added to DimRegion")
        
    except Exception as e:
        print(f"Loading error: {e}")