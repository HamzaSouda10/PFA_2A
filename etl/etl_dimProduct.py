import pandas as pd
from sqlalchemy import create_engine

def etl_dim_product(source_conn_str, target_conn_str):
    """
    Fonction complète ETL pour la table DimProduct.
    
    Args:
        source_conn_str (str): Chaîne de connexion à la source (AdventureWorks)
        target_conn_str (str): Chaîne de connexion au Data Warehouse
    """
    
    # 1. Extraction (E)
    try:
        # Connexion à la source
        source_engine = create_engine(source_conn_str)
        
        # Requête SQL pour extraire les données produits avec leurs catégories
        query = """
            SELECT 
                p.ProductID, 
                p.Name AS ProductName, 
                p.ProductNumber, 
                p.Color, 
                p.Size, 
                p.Weight, 
                p.ProductLine, 
                p.Class, 
                p.Style,
                psc.ProductSubcategoryID, 
                pc.ProductCategoryID
            FROM Production.Product p
            LEFT JOIN Production.ProductSubcategory psc 
                ON p.ProductSubcategoryID = psc.ProductSubcategoryID
            LEFT JOIN Production.ProductCategory pc 
                ON psc.ProductCategoryID = pc.ProductCategoryID
        """
        
        df_product = pd.read_sql(query, source_engine)
        print(f"Extraction réussie : {len(df_product)} produits trouvés")
        
    except Exception as e:
        print(f"Erreur lors de l'extraction : {e}")
        return

    # 2. Transformation (T)
    try:
        # Nettoyage des valeurs NULL
        df_product['Color'].fillna('N/A', inplace=True)
        df_product['Size'].fillna('Standard', inplace=True)
        df_product['Weight'].fillna(0, inplace=True)
        df_product['ProductLine'].fillna('Undefined', inplace=True)  # Ou 'N/A' si déjà utilisé ailleurs
        df_product['Class'].fillna('U', inplace=True)  # 'U' pour 'Unclassified'
        df_product['Style'].fillna('Undefined', inplace=True)  # Valeur par défaut réaliste
        
        # Gestion des clés étrangères manquantes
        df_product['ProductCategoryID'].fillna(-1, inplace=True)  # -1 = "Non catégorisé"
        df_product['ProductSubcategoryID'].fillna(-1, inplace=True)
        
        # Standardisation des textes
        df_product['ProductLine'] = df_product['ProductLine'].str.upper()
        df_product['Class'] = df_product['Class'].str.upper()
        df_product['Style'] = df_product['Style'].str.upper()
        
        print("Transformation terminée avec succès")
        
    except Exception as e:
        print(f"Erreur lors de la transformation : {e}")
        return

    # 3. Chargement (L)
    try:
        # Connexion au Data Warehouse
        target_engine = create_engine(target_conn_str)
        
        # Chargement dans DimProduct (mode append)
        df_product.to_sql(
            'DimProduct', 
            target_engine, 
            if_exists='append', 
            index=False,
           
        )
        print(f"Chargement réussi : {len(df_product)} lignes ajoutées à DimProduct")
        
    except Exception as e:
        print(f"Erreur lors du chargement : {e}")

