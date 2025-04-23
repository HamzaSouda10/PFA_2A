import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from sqlalchemy import Integer, Date, SmallInteger

def etl_dim_date(start_date, end_date, target_conn_str):
    """
    Génère et charge la table DimDate dans le Data Warehouse.
    
    Args:
        start_date (str): Date de début (format 'YYYY-MM-DD').
        end_date (str): Date de fin (format 'YYYY-MM-DD').
        target_conn_str (str): Chaîne de connexion au Data Warehouse.
    """
    # 1. Extraction + Transformation (génération du calendrier)
    try:
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        df_date = pd.DataFrame({
            'Date': date_range,
            'Day': date_range.day,
            'Month': date_range.month,
            'Year': date_range.year,
            'Quarter': date_range.quarter,
            'Week': date_range.isocalendar().week  # Numéro de semaine ISO
        })
        
        # Ajout de DateID (format YYYYMMDD)
        df_date['DateID'] = df_date['Date'].dt.strftime('%Y%m%d').astype(int)
        
        # Réorganisation des colonnes
        df_date = df_date[['DateID', 'Date', 'Day', 'Month', 'Year', 'Quarter', 'Week']]
        
        print(f"Génération réussie : {len(df_date)} dates créées ({start_date} à {end_date})")
    
    except Exception as e:
        print(f"Erreur lors de la génération du calendrier : {e}")
        return

    # 2. Chargement
    try:
        engine = create_engine(target_conn_str)
        
        # Définition des types SQL pour optimisation
        dtype = {
            'DateID': Integer,  # Utilisation de Integer de SQLAlchemy
            'Date': Date,       # Utilisation de Date de SQLAlchemy
            'Day': SmallInteger,  # Utilisation de SmallInteger de SQLAlchemy
            'Month': SmallInteger,
            'Year': SmallInteger,
            'Quarter': SmallInteger,
            'Week': SmallInteger
        }
        
        df_date.to_sql('DimDate', engine, if_exists='replace', index=False, dtype=dtype)
        print("Chargement réussi dans DimDate")
        
    except Exception as e:
        print(f"Erreur lors du chargement : {e}")