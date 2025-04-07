import pandas as pd
from sqlalchemy import create_engine

def etl_dim_product_category(source_conn_str, target_conn_str):
    """
    Charge la table DimProductCategory depuis AdventureWorks vers le Data Warehouse.
    
    Args:
        source_conn_str (str): Chaîne de connexion à la source (ex: SQL Server)
        target_conn_str (str): Chaîne de connexion au Data Warehouse (ex: PostgreSQL)
    """
    # 1. Extraction (E)
    try:
        source_engine = create_engine(source_conn_str)
        
        # Requête pour récupérer les catégories de produits
        query = """
            SELECT 
                ProductCategoryID AS CategoryID,
                Name AS CategoryName
            FROM Production.ProductCategory
        """
        
        df_category = pd.read_sql(query, source_engine)
        print(f"Extraction réussie : {len(df_category)} catégories trouvées")
    
    except Exception as e:
        print(f"Erreur lors de l'extraction : {e}")
        return

   

    # 3. Chargement (L)
    try:
        target_engine = create_engine(target_conn_str)
        
       
        
        # Chargement avec remplacement de la table existante
        df_category.to_sql(
            'DimProductCategory',
            target_engine,
            if_exists='replace',
            index=False,
            
        )
        print(f"Chargement réussi : {len(df_category)} catégories insérées")
    
    except Exception as e:
        print(f"Erreur lors du chargement : {e}")

# Exemple d'utilisation
if __name__ == "__main__":
    # Configuration des connexions (à adapter)
    SOURCE_DB = "sqlserver://user:password@server/AdventureWorks"
    TARGET_DB = "postgresql://dw_user:dw_password@dw-server/datawarehouse"
    
    # Exécution du processus ETL
    etl_dim_product_category(SOURCE_DB, TARGET_DB)