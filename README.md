# Data Preparation Pipeline: SQL Server para BigQuery

Este repositório contém o projeto final desenvolvido para a disciplina de **Data Preparation** do MBA em Big Data, Business Intelligence e Business Analytics da **Escola Politécnica da UFRJ (ITLAB)**.

O projeto implementa um pipeline de dados, utilizando uma **Arquitetura Medalhão** para integrar dados legados de um ERP (SQL Server) com logs de navegação em tempo real, gerando insights de conversão de marketing.

## 🚀 Visão Geral do Projeto

Uma startup de e-commerce enfrentava o desafio de "silos de dados". As vendas estavam presas em um banco SQL Server local, enquanto o comportamento dos usuários no site era gerado via APIs. Este pipeline unifica essas fontes no **Google BigQuery**, utilizando processamento distribuído para garantir escalabilidade.

### 🏗️ Arquitetura Medalhão
* **Camada Bronze (Raw):** Ingestão de dados brutos do SQL Server e logs.
* **Camada Silver (Trusted):** Limpeza de dados e aplicação de regras de **LGPD** (anonimização de clientes).
* **Camada Gold (Refined):** Processamento massivo via **Apache Spark (PySpark)** para cruzamento de dados e geração de insights de conversão.

## 🛠️ Tecnologias Utilizadas

* **Linguagem:** Python
* **Processamento:** Pandas (Ingestão/Migração) e Apache Spark (Transformação em escala).
* **Cloud:** Google Cloud Platform (BigQuery).
* **Automação & FinOps:** n8n e Google Sheets.
* **Banco de Origem:** Microsoft SQL Server (AdventureWorks2022)

## 💡 Diferenciais Técnicos

### 1. Estratégia FinOps (Billing Free)
Para viabilizar o desenvolvimento sem a necessidade de faturamento imediato no GCP, utilizamos o **n8n** para automatizar o input de logs no **Google Sheets**. O BigQuery consome esses dados como uma **External Table**, permitindo rodar o pipeline com custo zero de ingestão.

### 2. Conformidade com LGPD
Os scripts realizam a anonimização de dados sensíveis na origem. O campo `CustomerName` é transformado (FirstName + Inicial do LastName) antes mesmo de ser persistido na nuvem, garantindo a privacidade.

### 3. Integridade com PySpark
Na camada Gold, utilizamos o motor distribuído do Spark para realizar um **Left Semi Join**, garantindo que apenas interações web vinculadas a produtos reais do catálogo sejam contabilizadas, eliminando ruídos na análise de conversão.

## 📂 Estrutura do Repositório

* `migracao_sql.py`: Script de extração do SQL Server local e carga na Camada Bronze/Silver do BigQuery via Pandas.
* `analise_pyspark.py`: Script de processamento em escala na Camada Gold utilizando Spark para gerar insights de negócio.
* `Processo_Replicavel.docx`: Relatório técnico detalhado com as premissas e configurações do ambiente.

## ⚙️ Como Replicar

1.  **Pré-requisitos:** Ter o Python e o Spark instalados, além de uma instância do SQL Server com o banco.
2.  **Configuração GCP:** Crie um projeto no Google Cloud e gere uma chave JSON de uma *Service Account* com permissões de BigQuery.
3.  **Variáveis de Ambiente:** Substitua o caminho do arquivo `.json` nos scripts pelo seu arquivo de credenciais.
4.  **Execução:**
    * Rode `migracao_sql.py` para migrar o legado.
    * Configure o n8n ou alimente o Google Sheets com logs fictícios.
    * Rode `analise_pyspark.py` para gerar a tabela de insights.

## 📜 Referência Acadêmica

Este projeto faz parte do portfólio acadêmico do grupo abaixo:
Carlos Henrique Ramalho P Linhares
Eralda Ferreira da Silva
Felipe Nascimento da Cruz
Flávia Lucena de Araújo
Rafael Castilho Freire

---
⭐ *Se este projeto foi útil para seus estudos, sinta-se à vontade para contribuir!*