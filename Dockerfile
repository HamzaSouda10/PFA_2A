FROM apache/airflow:2.7.0-python3.10

# Passer à l'utilisateur root pour installer des dépendances système
USER root

# Mise à jour des paquets et installation des dépendances
RUN apt-get update && apt-get install -y \
    unixodbc \
    gcc \
    g++ \
    gnupg \
    curl \
    apt-transport-https \
    ca-certificates \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17


# Revenir à l'utilisateur airflow pour la suite des opérations
USER airflow

# Installation des paquets Python nécessaires via pip
RUN pip install pyodbc sqlalchemy pandas
