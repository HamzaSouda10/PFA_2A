import pandas as pd
from sqlalchemy import create_engine, String, Date, Integer

def etl_dim_employee(source_conn_str, target_conn_str):
    try:
        # Connexion à la base source (AdventureWorks)
        source_engine = create_engine(source_conn_str)
        
        # Requête SQL pour extraire les employés
        query = """
            SELECT
                e.BusinessEntityID AS EmployeeID,
                p.FirstName,
                p.LastName,
                e.JobTitle AS Title,
                e.HireDate,
                e.BirthDate,
                e.Gender,
                e.MaritalStatus,
                ea.EmailAddress,
                pp.PhoneNumber AS Phone
            FROM HumanResources.Employee e
            JOIN Person.Person p ON e.BusinessEntityID = p.BusinessEntityID
            LEFT JOIN Person.EmailAddress ea ON p.BusinessEntityID = ea.BusinessEntityID
            LEFT JOIN Person.PersonPhone pp ON p.BusinessEntityID = pp.BusinessEntityID
        """
        
        df_employee = pd.read_sql(query, source_engine)
        print(f"Extraction successful: {len(df_employee)} employees found")
        
    except Exception as e:
        print(f"Extraction error: {e}")
        return

    try:
        # Connexion à la base cible (Data Warehouse)
        target_engine = create_engine(target_conn_str)
        
        # Définition des types SQL explicites
        dtype_mapping = {
            'EmployeeID': Integer,
            'FirstName': String(50),
            'LastName': String(50),
            'Title': String(100),
            'HireDate': Date,
            'BirthDate': Date,
            'Gender': String(1),
            'MaritalStatus': String(1),
            'EmailAddress': String(100),
            'Phone': String(25)
        }
        
        # Chargement dans la table DimEmployee
        df_employee.to_sql(
            'DimEmployee',
            target_engine,
            if_exists='replace',
            index=False,
            dtype=dtype_mapping
        )
        print(f"Loading successful: {len(df_employee)} rows added to DimEmployee")
        
    except Exception as e:
        print(f"Loading error: {e}")
