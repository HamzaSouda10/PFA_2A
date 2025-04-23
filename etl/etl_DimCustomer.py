import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import create_engine, String, Integer

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
            FROM AdventureWorks2022.Sales.Customer AS c
                LEFT JOIN AdventureWorks2022.Person.Person AS p 
                    ON c.PersonID = p.BusinessEntityID
                LEFT JOIN AdventureWorks2022.Sales.Store AS s 
                    ON c.StoreID = s.BusinessEntityID
                CROSS APPLY (
                    SELECT 
                        CASE 
                            WHEN p.BusinessEntityID IS NOT NULL THEN p.BusinessEntityID 
                            ELSE s.BusinessEntityID 
                        END AS BEID
                ) AS be
                LEFT JOIN AdventureWorks2022.Person.EmailAddress AS e 
                    ON e.BusinessEntityID = be.BEID
                LEFT JOIN AdventureWorks2022.Person.PersonPhone AS ph 
                    ON ph.BusinessEntityID = be.BEID
                LEFT JOIN AdventureWorks2022.Person.BusinessEntityAddress AS bea 
                    ON bea.BusinessEntityID = be.BEID
                LEFT JOIN AdventureWorks2022.Person.Address AS a 
                    ON a.AddressID = bea.AddressID
                LEFT JOIN AdventureWorks2022.Person.StateProvince AS sp 
                    ON sp.StateProvinceID = a.StateProvinceID
                LEFT JOIN AdventureWorks2022.Person.CountryRegion AS cr 
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
        
                # Define explicit data types for SQL Server
        dtype_mapping = {
            'CustomerID': Integer,  # Integer pour CustomerID
            'FirstName': String(50),  # NVARCHAR(50) pour FirstName
            'LastName': String(50),  # NVARCHAR(50) pour LastName
            'CompanyName': String(100),  # NVARCHAR(100) pour CompanyName
            'EmailAddress': String(100),  # NVARCHAR(100) pour EmailAddress
            'Phone': String(25),  # NVARCHAR(25) pour Phone
            'AddressLine1': String(100),  # NVARCHAR(100) pour AddressLine1
            'AddressLine2': String(100),  # NVARCHAR(100) pour AddressLine2
            'City': String(50),  # NVARCHAR(50) pour City
            'StateProvince': String(50),  # NVARCHAR(50) pour StateProvince
            'CountryRegion': String(50),  # NVARCHAR(50) pour CountryRegion
        }
        
        # Load into DimCustomer (append mode)
        df_customer.to_sql(
            'DimCustomer', 
            target_engine, 
            if_exists='replace', 
            index=False,
            dtype=dtype_mapping
            )
        print(f"Loading successful: {len(df_customer)} rows added to DimCustomer")
        
    except Exception as e:
        print(f"Loading error: {e}")