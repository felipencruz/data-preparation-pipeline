import os
import pandas as pd
from pyspark.sql import SparkSession
from google.cloud import bigquery
from google.oauth2 import service_account

path_to_json = "credenciais-bigquery.json"

scopes = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery"
]

credentials = service_account.Credentials.from_service_account_file(
    path_to_json, scopes=scopes
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# INICIALIZANDO O MOTOR SPARK
spark = SparkSession.builder \
    .appName("Analise_GlobalStore_MBA") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

print("\n" + "="*50)
print("--- ETAPA 1: CAPTURA DE DADOS (EXTRAÇÃO) ---")
print("="*50)

# Buscando os Logs
query_logs = "SELECT * FROM `dataset-google.data_preparation.logs_navegacao`"
query_vendas = "SELECT * FROM `dataset-google.data_preparation.fact_sales`"

try:
    print("Acessando logs no Google Sheets via BigQuery...")
    pdf_logs = client.query(query_logs).to_dataframe()
    
    print("Acessando vendas históricas no BigQuery...")
    pdf_vendas = client.query(query_vendas).to_dataframe()
    
    # 3. CONVERTENDO PARA SPARK DATAFRAMES
    df_logs = spark.createDataFrame(pdf_logs)
    df_vendas = spark.createDataFrame(pdf_vendas)

    print("\n" + "="*50)
    print("--- ETAPA 2: PROCESSAMENTO NO PYSPARK ---")
    print("="*50)

    # Cruzando Navegação com Venda
    df_consolidado = df_logs.join(df_vendas, df_logs.product_key == df_vendas.ProductKey, "left_semi")

    # Gerando o Insight: Contagem de eventos que resultaram em correlação de venda
    insight = df_consolidado.groupBy("event_type").count().withColumnRenamed("count", "Total_Interacoes")

    print("RESULTADO DO PROCESSAMENTO SPARK:")
    insight.show()

    print("\n" + "="*50)
    print("--- ETAPA 3: PERSISTÊNCIA (CAMADA GOLD) ---")
    print("="*50)

    # Convertendo de volta para salvar no BigQuery
    insight_final = insight.toPandas()
    insight_final.to_gbq("data_preparation.insight_conversao", 
                         project_id=credentials.project_id, 
                         if_exists="replace",
                         credentials=credentials)

    print("SUCESSO! Tabela 'insight_conversao' atualizada no BigQuery.")

except Exception as e:
    print(f"\nERRO IDENTIFICADO: {e}")

finally:
    spark.stop()