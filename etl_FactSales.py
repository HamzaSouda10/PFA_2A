from pandas import pd
from sqlalchemy import create_engine

def etl_fact_sales(source_conn_str, target_conn_str):
    try:
        # Establish source connection
        source_engine = create_engine(source_conn_str)
        
        # SQL query to extract sales transaction data
        query = """
            SELECT 
                ROW_NUMBER() OVER (ORDER BY h.SalesOrderID) AS SaleID,
                h.SalesOrderID,             
                d.ProductID,                 
                h.CustomerID,              
                h.SalesPersonID,            
                d.SpecialOfferID,           
                h.OrderDate,                
                h.ShipDate,                 
                h.TerritoryID,              
                d.OrderQty,
                d.UnitPrice,
                d.UnitPriceDiscount,
                d.LineTotal,
                h.Freight,
                h.TotalDue
            FROM AdventureWorks.Sales.SalesOrderHeader AS h
            JOIN AdventureWorks.Sales.SalesOrderDetail AS d 
                ON h.SalesOrderID = d.SalesOrderID
        """
        
        df_sales = pd.read_sql(query, source_engine)
        print(f"Extraction successful: {len(df_sales)} sales transactions found")
        
    except Exception as e:
        print(f"Extraction error: {e}")
        return

    try:
        # Establish target connection
        target_engine = create_engine(target_conn_str)
        
        # Load into FactSales
        df_sales.to_sql(
            'FactSales',
            target_engine,
            if_exists='append',
            index=False,
            method='multi',  # For better performance with bulk inserts
            chunksize=1000
        )
        print(f"Loading successful: {len(df_sales)} rows added to FactSales")
        
    except Exception as e:
        print(f"Loading error: {e}")