from pandas import pd
from sqlalchemy import create_engine
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
            FROM [AdventureWorks].[Sales].[SpecialOffer]
        """
        
        df_offers = pd.read_sql(query, source_engine)
        print(f"Extraction successful: {len(df_offers)} special offers found")
        
    except Exception as e:
        print(f"Extraction error: {e}")
        return

    try:
        # Establish target connection
        target_engine = create_engine(target_conn_str)
        
        # Load into DimSpecialOffer
        df_offers.to_sql(
            'DimSpecialOffer',
            target_engine,
            if_exists='append',
            index=False
        )
        print(f"Loading successful: {len(df_offers)} rows added to DimSpecialOffer")
        
    except Exception as e:
        print(f"Loading error: {e}")