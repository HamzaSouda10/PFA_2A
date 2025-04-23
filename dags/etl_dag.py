from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl.etl_dimDate import etl_dim_date  # importe ta fonction ETL
from etl.etl_dimProduct import etl_dim_product
from etl.etl_dimProductCategory import etl_dim_product_category
from etl.etl_dimProductSubCategory import etl_dim_product_subcategory
from etl.etl_FactProduction import etl_fact_production
from etl.etl_DimCustomer import etl_dim_customer
from etl.etl_DimRegion import etl_dim_region
from etl.etl_DimSpecialOffer import etl_dim_special_offer
from etl.etl_FactSales import etl_fact_sales

# 1. Connexion à la source (AdventureWorks)
server_name = "host.docker.internal,1433"  # Ex: "localhost\\SQLEXPRESS"
database_name = "AdventureWorks2022"

# Connexions (remplace par les vraies infos)
SOURCE_CONN_STR = (
    f"mssql+pyodbc://etl_user:yourStrongPassword123!@{server_name}/{database_name}?"
    "driver=ODBC+Driver+17+for+SQL+Server"
)
# 2. Connexion au Data Warehouse (cible)
target_server = "host.docker.internal,1433"
target_database = "PFA_DW"
TARGET_CONN_STR = (
     f"mssql+pyodbc://etl_user:yourStrongPassword123!@{target_server}/{target_database}?"
    "driver=ODBC+Driver+17+for+SQL+Server"
)
start_date = '2010-01-01'
end_date = '2024-12-31'
# Wrappers
def run_etl_dim_date():
    etl_dim_date(start_date, end_date, TARGET_CONN_STR)

def run_etl_dim_product():
    etl_dim_product(SOURCE_CONN_STR, TARGET_CONN_STR)

def run_etl_dim_product_category():
    etl_dim_product_category(SOURCE_CONN_STR, TARGET_CONN_STR)

def run_etl_dim_product_subcategory():
    etl_dim_product_subcategory(SOURCE_CONN_STR, TARGET_CONN_STR)

def run_etl_dim_customer():
    etl_dim_customer(SOURCE_CONN_STR, TARGET_CONN_STR)

def run_etl_dim_region():
    etl_dim_region(SOURCE_CONN_STR, TARGET_CONN_STR)

def run_etl_dim_special_offer():
    etl_dim_special_offer(SOURCE_CONN_STR, TARGET_CONN_STR)

def run_etl_fact_production():
    etl_fact_production(SOURCE_CONN_STR, TARGET_CONN_STR)

def run_etl_fact_sales():
    etl_fact_sales(SOURCE_CONN_STR, TARGET_CONN_STR)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='adventureworks_etl_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['adventureworks']
) as dag:

    # Dimension tables
    task_dim_date = PythonOperator(
        task_id='load_dim_date',
        python_callable=run_etl_dim_date
    )

    task_dim_product_category = PythonOperator(
        task_id='load_dim_product_category',
        python_callable=run_etl_dim_product_category
    )

    task_dim_product_subcategory = PythonOperator(
        task_id='load_dim_product_subcategory',
        python_callable=run_etl_dim_product_subcategory
    )

    task_dim_product = PythonOperator(
        task_id='load_dim_product',
        python_callable=run_etl_dim_product
    )

    task_dim_customer = PythonOperator(
        task_id='load_dim_customer',
        python_callable=run_etl_dim_customer
    )

    task_dim_region = PythonOperator(
        task_id='load_dim_region',
        python_callable=run_etl_dim_region
    )

    task_dim_special_offer = PythonOperator(
        task_id='load_dim_special_offer',
        python_callable=run_etl_dim_special_offer
    )

    # Fact tables
    task_fact_production = PythonOperator(
        task_id='load_fact_production',
        python_callable=run_etl_fact_production
    )

    task_fact_sales = PythonOperator(
        task_id='load_fact_sales',
        python_callable=run_etl_fact_sales
    )

    # Dépendances logiques (exemple simple)
    task_dim_date >> [task_dim_product_category, task_dim_customer, task_dim_region, task_dim_special_offer]

    task_dim_product_category >> task_dim_product_subcategory >> task_dim_product

    task_dim_product >> task_fact_production
    task_dim_customer >> task_fact_sales
    task_dim_region >> task_fact_sales
    task_dim_special_offer >> task_fact_sales
    task_dim_date >> [task_fact_production, task_fact_sales]