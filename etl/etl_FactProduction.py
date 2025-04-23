import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import create_engine, Integer, DateTime, Numeric

def etl_fact_production(source_conn_str, target_conn_str):
    """
    Charge la table FactProduction avec calcul du AverageCost manquant.
    
    Args:
        source_conn_str (str): Chaîne de connexion SQL Server
        target_conn_str (str): Chaîne de connexion Data Warehouse
    """
    # 1. Extraction (E)
    try:
        source_engine = create_engine(source_conn_str)
        
        # Requête pour récupérer les données de coûts
        query = """
            SELECT 
                pch.ProductID,
                pch.StartDate,
                pch.EndDate,
                pch.StandardCost,
                p.ListPrice  -- Pour calculer AverageCost
            FROM Production.ProductCostHistory pch
            JOIN Production.Product p ON pch.ProductID = p.ProductID
            WHERE pch.StartDate IS NOT NULL
        """
        
        df = pd.read_sql(query, source_engine)
        print(f"Extraction réussie : {len(df)} enregistrements")
    
    except Exception as e:
        print(f"Erreur d'extraction : {e}")
        return

    # 2. Transformation (T)
    try:
        # Calcul du AverageCost (moyenne entre StandardCost et ListPrice)
        df['AverageCost'] = (df['StandardCost'] + df['ListPrice']) / 2
        
        # Conversion des dates en DateID
        df['StartDateID'] = pd.to_datetime(df['StartDate']).dt.strftime('%Y%m%d').astype(int)
        # Gérer les EndDate nulles en les remplaçant par -1 dans EndDateID
        df['EndDateID'] = pd.to_datetime(df['EndDate'], errors='coerce')  # convertit NaT si problème
        df['EndDateID'] = df['EndDateID'].dt.strftime('%Y%m%d')
        df['EndDateID'] = df['EndDateID'].fillna(-1).astype(int)
        
        # Nettoyage
        df['StandardCost'] = df['StandardCost'].fillna(0).round(4)
        df['AverageCost'] = df['AverageCost'].fillna(0).round(4)
        
        # Validation des clés
        target_engine = create_engine(target_conn_str)
        valid_products = pd.read_sql("SELECT ProductID FROM DimProduct", target_engine)['ProductID'].tolist()
        df = df[df['ProductID'].isin(valid_products)]
        
        print("Transformation terminée")
    
    except Exception as e:
        print(f"Erreur de transformation : {e}")
        return

    # 3. Chargement (L)
    try:
        # Structure finale
        df_final = df[[
            'ProductID',
            'StartDateID',
            'EndDateID',
            'StandardCost',
            'AverageCost'
        ]]
        dtype_mapping = {
            'ProductID': Integer,  # Integer pour ProductID
            'StartDateID': Integer,  # Integer pour StartDateID
            'EndDateID': Integer,  # Integer pour EndDateID
            'StandardCost': Numeric(19, 4),  # DECIMAL(19,4) pour StandardCost
            'AverageCost': Numeric(19, 4)  # DECIMAL(19,4) pour AverageCost
    
        }

       
        
        # Chargement
        df_final.to_sql(
            'FactProduction',
            target_engine,
            if_exists='replace',
            index=False,
            dtype=dtype_mapping
            
        )
        print(f"Chargement réussi : {len(df_final)} lignes")
    
    except Exception as e:
        print(f"Erreur de chargement : {e}")

