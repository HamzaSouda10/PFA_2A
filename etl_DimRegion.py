from pandas import pd
from sqlalchemy import create_engine


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
            FROM [AdventureWorks].[Sales].[SalesTerritory]
        """
        
        df_region = pd.read_sql(query, source_engine)
        print(f"Extraction successful: {len(df_region)} sales territories found")
        
    except Exception as e:
        print(f"Extraction error: {e}")
        return

    try:
        # Establish target connection
        target_engine = create_engine(target_conn_str)
        
        # Load into DimRegion
        df_region.to_sql(
            'DimRegion',
            target_engine,
            if_exists='append',
            index=False
        )
        print(f"Loading successful: {len(df_region)} rows added to DimRegion")
        
    except Exception as e:
        print(f"Loading error: {e}")