import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import create_engine, Integer, Numeric, String, DateTime

def etl_dim_special_offer(source_conn_str, target_conn_str):
    try:
        # Establish source connection
        source_engine = create_engine(source_conn_str)
        
        # SQL query to extract special offer data
        query = """
            SELECT 
                [SpecialOfferID],
                [Description],
                [DiscountPct],
                [Type],
                [Category],
                [StartDate],
                [EndDate],
                [MinQty],
                [MaxQty]
            FROM [AdventureWorks2022].[Sales].[SpecialOffer]
        """
        
        df_offers = pd.read_sql(query, source_engine)
        print(f"Extraction successful: {len(df_offers)} special offers found")
        
    except Exception as e:
        print(f"Extraction error: {e}")
        return

    try:
        # Establish target connection
        target_engine = create_engine(target_conn_str)
        
        # Définition des types de données SQL Server explicites
        dtype_mapping = {
            'SpecialOfferID': Integer,  # Integer pour SpecialOfferID
            'Description': String(255),  # NVARCHAR(255) pour Description
            'DiscountPct': Numeric(5, 2),  # DECIMAL(5,2) pour DiscountPct
            'Type': String(50),  # NVARCHAR(50) pour Type
            'Category': String(50),  # NVARCHAR(50) pour Category
            'StartDate': DateTime,  # DATETIME pour StartDate
            'EndDate': DateTime,  # DATETIME pour EndDate
            'MinQty': Integer,  # INT pour MinQty
            'MaxQty': Integer,  # INT pour MaxQty
        }
        # Load into DimSpecialOffer
        df_offers.to_sql(
            'DimSpecialOffer',
            target_engine,
            if_exists='replace',
            index=False,
            dtype=dtype_mapping
        )
        print(f"Loading successful: {len(df_offers)} rows added to DimSpecialOffer")
        
    except Exception as e:
        print(f"Loading error: {e}")