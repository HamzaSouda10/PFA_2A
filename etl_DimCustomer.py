import pandas as pd
from sqlalchemy import create_engine

def etl_dim_customer(source_conn_str, target_conn_str):
    try:
        # Source connection
        source_engine = create_engine(source_conn_str)
        
        # SQL query to extract customer data with related information
        query = """
            SELECT 
                c.CustomerID,
                p.FirstName,
                p.LastName,
                s.Name AS CompanyName,
                e.EmailAddress,
                ph.PhoneNumber AS Phone,
                a.AddressLine1,
                a.AddressLine2,
                a.City,
                sp.Name AS StateProvince,
                cr.Name AS CountryRegion
            FROM AdventureWorks.Sales.Customer AS c
                LEFT JOIN AdventureWorks.Person.Person AS p 
                    ON c.PersonID = p.BusinessEntityID
                LEFT JOIN AdventureWorks.Sales.Store AS s 
                    ON c.StoreID = s.BusinessEntityID
                CROSS APPLY (
                    SELECT 
                        CASE 
                            WHEN p.BusinessEntityID IS NOT NULL THEN p.BusinessEntityID 
                            ELSE s.BusinessEntityID 
                        END AS BEID
                ) AS be
                LEFT JOIN AdventureWorks.Person.EmailAddress AS e 
                    ON e.BusinessEntityID = be.BEID
                LEFT JOIN AdventureWorks.Person.PersonPhone AS ph 
                    ON ph.BusinessEntityID = be.BEID
                LEFT JOIN AdventureWorks.Person.BusinessEntityAddress AS bea 
                    ON bea.BusinessEntityID = be.BEID
                LEFT JOIN AdventureWorks.Person.Address AS a 
                    ON a.AddressID = bea.AddressID
                LEFT JOIN AdventureWorks.Person.StateProvince AS sp 
                    ON sp.StateProvinceID = a.StateProvinceID
                LEFT JOIN AdventureWorks.Person.CountryRegion AS cr 
                    ON cr.CountryRegionCode = sp.CountryRegionCode
        """
        
        df_customer = pd.read_sql(query, source_engine)
        print(f"Extraction successful: {len(df_customer)} customers found")
        
    except Exception as e:
        print(f"Extraction error: {e}")
        return

    try:
        # Target connection
        target_engine = create_engine(target_conn_str)
        
        # Load into DimCustomer (append mode)
        df_customer.to_sql(
            'DimCustomer', 
            target_engine, 
            if_exists='append', 
            index=False
        )
        print(f"Loading successful: {len(df_customer)} rows added to DimCustomer")
        
    except Exception as e:
        print(f"Loading error: {e}")