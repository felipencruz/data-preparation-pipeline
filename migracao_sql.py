# instalado no terminal anes de rodar > pip install pandas pyodbc pandas-gbq google-cloud-bigquery #

import pandas as pd
import pyodbc
from google.cloud import bigquery
import os

# caminho do JSON da conta de serviço criada no GCP pra autenticar
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credenciais-bigquery.json"

# Configurada a conexão com o SQL local (SERVIDOR-SQL) no banco
conn_str = (
    "Driver={SQL Server};"
    "Server=SERVIDOR-SQL"
    "Database=BANCO;"
    "Trusted_Connection=yes;"
)

# Inicializado o client do BigQuery com o ID do projeto
client = bigquery.Client()
project_id = "projeto-google"
dataset_id = "dataset-google"

def migrar_tabela(nome_tabela, query):
    print(f"--- Iniciando extração de {nome_tabela} ---")
    conn = pyodbc.connect(conn_str)
    df = pd.read_sql(query, conn)
    
    # ETL pra LGPD
    if 'FirstName' in df.columns:
        df['CustomerName'] = df['FirstName'] + " " + df['LastName'].str[0] + "."
        df = df.drop(['FirstName', 'LastName'], axis=1)
        print("Transformação: Nomes de clientes anonimizados.")

    # destino pra empurrar o DataFrame direto pro BigQuery
    table_ref = f"{project_id}.{dataset_id}.{nome_tabela}"
    
    df.to_gbq(table_ref, project_id=project_id, if_exists='replace')
    print(f"Sucesso! Tabela {nome_tabela} carregada no BigQuery.\n")
    conn.close()

# 5 tabelas porque são o core do DW pra cruzar com os logs depois
tabelas = {
    "dim_product": "SELECT ProductKey, EnglishProductName, StandardCost FROM dbo.DimProduct",
    "dim_customer": "SELECT CustomerKey, FirstName, LastName, BirthDate, MaritalStatus FROM dbo.DimCustomer",
    "fact_sales": "SELECT ProductKey, OrderDateKey, CustomerKey, SalesAmount, OrderQuantity FROM dbo.FactInternetSales",
    "dim_territory": "SELECT SalesTerritoryKey, SalesTerritoryRegion, SalesTerritoryCountry FROM dbo.DimSalesTerritory",
    "dim_category": "SELECT ProductCategoryKey, EnglishProductCategoryName FROM dbo.DimProductCategory"
}

# loop pra processar todas as tabelas de uma vez
for nome, sql in tabelas.items():
    migrar_tabela(nome, sql)