import pandas as pd
from sqlalchemy import create_engine

def etl_dim_product_subcategory(source_conn_str, target_conn_str):
    """
    Charge la table DimProductSubcategory depuis la source vers le Data Warehouse.
    
    Args:
        source_conn_str (str): Chaîne de connexion à la base AdventureWorks.
        target_conn_str (str): Chaîne de connexion au Data Warehouse.
    """
    # 1. Extraction (E)
    try:
        # Connexion à la source
        source_engine = create_engine(source_conn_str)
        
        # Requête SQL pour récupérer les sous-catégories et leurs catégories parentes
        query = """
            SELECT 
                psc.ProductSubcategoryID AS SubcategoryID,
                psc.Name AS SubcategoryName,
                psc.ProductCategoryID AS CategoryID
            FROM Production.ProductSubcategory psc
        """
        
        df_subcategory = pd.read_sql(query, source_engine)
        print(f"Extraction réussie : {len(df_subcategory)} sous-catégories trouvées")
    
    except Exception as e:
        print(f"Erreur lors de l'extraction : {e}")
        return

    # 2. Transformation (T)
    try:
        # Nettoyage des valeurs manquantes (si nécessaire)
        df_subcategory['SubcategoryName'].fillna('Unknown', inplace=True)
        
        # Gestion des CategoryID NULL (ex: sous-catégorie orpheline)
        df_subcategory['CategoryID'].fillna(-1, inplace=True)  # -1 = "Non catégorisé"
        
        # Standardisation du texte
        df_subcategory['SubcategoryName'] = df_subcategory['SubcategoryName'].str.strip().str.title()
        
        print("Transformation terminée avec succès")
    
    except Exception as e:
        print(f"Erreur lors de la transformation : {e}")
        return

    # 3. Chargement (L)
    try:
        # Connexion au Data Warehouse
        target_engine = create_engine(target_conn_str)
        
        # Définition des types SQL pour optimisation
       
        
        # Chargement dans DimProductSubcategory (remplace la table existante)
        df_subcategory.to_sql(
            'DimProductSubcategory', 
            target_engine, 
            if_exists='replace', 
            index=False,
            
        )
        print(f"Chargement réussi : {len(df_subcategory)} sous-catégories ajoutées")
    
    except Exception as e:
        print(f"Erreur lors du chargement : {e}")