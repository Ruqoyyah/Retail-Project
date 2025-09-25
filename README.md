## This project demonstrates how to build a Bronze–Silver–Gold data pipeline in Azure Databricks using PySpark and SQL.

### It covers:

- Mounting Azure Data Lake Storage (ADLS Gen2)
- Ingesting raw data (Bronze layer)
- Cleaning and transforming data (Silver layer)
- Creating curated views for analytics (Gold layer)

### Prerequisites:

- Azure Databricks workspace
- Azure Data Lake Storage (Gen2) account
- Service Principal with Storage Blob Data Contributor role
- Databricks Secret Scope configured to store client ID, client secret, and tenant ID


### Configuration & Mounting

####First, configure OAuth and mount ADLS:

```python

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": "<application-id>",
    "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="<scope-name>", key="<service-credential-key-name>"),
    "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"
}

dbutils.fs.mount(
    source="abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
    mount_point="/mnt/storagename/container",
    extra_configs=configs
)

dbutils.fs.ls('/mnt/storagename/')

`
### Bronze Layer (Raw Data)

### Read the raw dataset from the mounted storage:

```bronze_df = spark.read.parquet("/mnt/storagename/bronze")
display(bronze_df)

### Gold Layer (Analytics Views)

#####Load Silver data and register a temporary view:

```python
silver_df = spark.read.parquet("/mnt/storagename/silver/")
silver_df.createOrReplaceTempView("retail_d`ata")


### Run analytics with SQL:

```sql
-- Daily Revenue
SELECT DATE(TransactionDate) AS txn_date,
       SUM(amount) AS total_revenue,
       COUNT(DISTINCT TransactionID) AS total_purchase
FROM retail_data
GROUP BY DATE(TransactionDate)
ORDER BY txn_date;

-- Revenue by Payment Type
SELECT SUM(amount) AS total_revenue, PaymentType
FROM retail_data
GROUP BY PaymentType;

-- Store Performance
SELECT SUM(amount) AS total_revenue, StoreLocation
FROM retail_data
GROUP BY StoreLocation;

-- Loyalty Level Contribution
SELECT SUM(amount) AS total_revenue, CustomerLoyaltyLevel
FROM retail_data
GROUP BY CustomerLoyaltyLevel;

-- Product Category Sales
SELECT SUM(amount) AS total_revenue, ProductCategory
FROM retail_data
GROUP BY ProductCategory;


### Notes
### Replace placeholders (<application-id>, <scope-name>, <storage-account-name>, etc.) with your Azure details.

