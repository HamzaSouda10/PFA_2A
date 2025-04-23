import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import create_engine, Integer, SmallInteger, DateTime, Numeric

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
            FROM AdventureWorks2022.Sales.SalesOrderHeader AS h
            JOIN AdventureWorks2022.Sales.SalesOrderDetail AS d 
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
        
        # Définition des types de données SQL Server explicites
        dtype_mapping = {
            'SaleID': Integer,
            'SalesOrderID': Integer,
            'ProductID': Integer,
            'CustomerID': Integer,
            'SalesPersonID': Integer,
            'SpecialOfferID': Integer,
            'OrderDate': DateTime,
            'ShipDate': DateTime,
            'TerritoryID': Integer,
            'OrderQty': SmallInteger,
            'UnitPrice': Numeric(precision=19, scale=4),  # MONEY en SQL Server
            'UnitPriceDiscount': Numeric(precision=19, scale=4),  # MONEY en SQL Server
            'LineTotal': Numeric(precision=19, scale=4),  # MONEY en SQL Server
            'Freight': Numeric(precision=19, scale=4),  # MONEY en SQL Server
            'TotalDue': Numeric(precision=19, scale=4)  # MONEY en SQL Server
        }
        # Load into FactSales
        df_sales.to_sql(
            'FactSales',
            target_engine,
            if_exists='replace',
            index=False,
            dtype=dtype_mapping
            
        )
        print(f"Loading successful: {len(df_sales)} rows added to FactSales")
        
    except Exception as e:
        print(f"Loading error: {e}")