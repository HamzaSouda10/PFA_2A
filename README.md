# 🛠️ Data Engineering Project – Interactive Dashboard Project – AdventureWorks

## Project Overview
This project demonstrates a complete **Data Engineering workflow** on the **AdventureWorks** transactional database.  
The main goal is to **transform complex transactional data into a fully structured Data Warehouse**, ready for analysis, and then make it available for **interactive dashboards**.

The project focuses on:

* Creating **ETL functions** for each fact and dimension table  
* **Automating and orchestrating** the pipeline with **Apache Airflow**  
* Loading the processed data into a **SQL Server Data Warehouse**  
* Building **interactive dashboards** in Power BI to explore key metrics  

---

## Data Engineering Architecture

### 1. Data Source
* **Transactional Database**: AdventureWorks (SQL Server)  
* Contains data about:
  * Orders  
  * Products  
  * Customers  
  * Employees  
  * Inventory  

### 2. ETL (Extract – Transform – Load)
* Dedicated **Python ETL functions** for each table:
  * **Dimensions**: DimProduct, DimCustomer, DimEmployee, DimDate, DimRegion, DimLocation, DimSpecialOffer…  
  * **Facts**: FactSales, FactInventory, FactProduction  
* Each function performs:
  * **Extract**: pull raw data from SQL Server  
  * **Transform**: clean data, handle duplicates, standardize types, compute aggregations  
  * **Load**: write transformed tables to the SQL Server Data Warehouse  


### 3. Pipeline Orchestration with Airflow
* **Apache Airflow** manages the ETL pipelines:
  * Each ETL function is integrated into a main **DAG**  
  * **Automated and scheduled execution**  
  * **Dependency management** between fact and dimension tables  
  * Centralized **monitoring and logging**  

### 4. Data Warehouse
* Data is stored in **SQL Server**, modeled as a **constellation schema**, combining:
  * **Star schema** for primary fact tables and their dimensions  
  * **Snowflake schema** for normalized supporting dimensions  

### 5. Interactive Visualization (Power BI)
* The processed Data Warehouse is used to build **dynamic and interactive dashboards** in Power BI  
* Users can explore **KPIs and metrics** such as revenue, sales growth, inventory levels, employee productivity, and promotion effectiveness  
* Provides a **professional and insightful data exploration experience**  

---

## ETL Pipeline Summary
1. **ETL Functions** – Extract, Transform, Load for each fact and dimension  
2. **Airflow DAGs** – Automated orchestration and scheduling  
3. **SQL Server Data Warehouse** – Constellation model combining star and snowflake schemas  
4. **Interactive Dashboards** – Power BI visualization of key metrics  

---


