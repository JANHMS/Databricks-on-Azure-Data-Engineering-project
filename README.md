# Databricks Certified Data Engineer Associate Hands on projects

This repository aims to learn and showcase some features of Databricks.
This is used as hands on preparation for Databricks Data Engineer Associate certification exam.

To import these resources into your Databricks workspace, clone this repository via Databricks Repos.
## 0. Databricks + Pyspark + Azure
- Storing data in the FileStore of Databricks, loading into Workspace notebook and perfroming data science.
- Storing Data in Azure Blob and mounting to Databricks. This includes the following steps:
1. Create Resource Group in Azure.
2. Create Storage account and assign to Resouce group.
3. App registration (create a managed itenditiy), which we will use to connect Databricks to storage account.
3.1 Create a client secret and copy.
4. Create Key vault (assign to same resource group)
4.1. Add the cleint secret here.
5. Create secret scope within Databricks.
5.1 Use the keyvault DNS (url) and the ResourceID to allow Databricks to access the key valuts secrets within a specific scope.
6. Use this scope to retreive secrets and connect to storage acount container, where data is stored in Azure:

```
configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "<appId>",
       "fs.azure.account.oauth2.client.secret": "<clientSecret>",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<tenant>/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}
```

7. Finally we can mount the data:
```
dbutils.fs.mount(
source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/folder1",
mount_point = "/mnt/flightdata",
extra_configs = configs)
```
8. Now we can load the data from the MountPoint into a Dataframe and perform actions.

```
flightDF = spark.read.format('csv').options(
    header='true', inferschema='true').load("/mnt/flightdata/*.csv")

# read the airline csv file and write the output to parquet format for easy query.
flightDF.write.mode("append").parquet("/mnt/flightdata/parquet/flights")
print("Done")
```

## 1. Delta Lake in Lakehouse

Working with Delta Tables and apply some transformations such as ZODRDER or OPTIMIZE.

<img width="453" alt="Screenshot 2023-01-23 at 07 19 38" src="https://user-images.githubusercontent.com/45521680/213977182-a7e8efb6-96e2-4abd-a22c-dfece7f44ab4.png">!

<img width="277" alt="Screenshot 2023-01-23 at 07 20 35" src="https://user-images.githubusercontent.com/45521680/213977219-ea46f78d-eb32-4e33-a920-39b781a2c860.png">

## 2. ETL with Pyspark in Databricks
<img width="639" alt="Screenshot 2023-01-22 at 14 22 36" src="https://user-images.githubusercontent.com/45521680/213929800-d36f6d0d-23b4-4fe3-8993-2d1da2d89f46.png">

## 3.Incremental Data Processing
Using AutoLoader and COPY to process incremental Data Processing, though steaming.
<img width="542" alt="Screenshot 2023-01-23 at 07 24 57" src="https://user-images.githubusercontent.com/45521680/213977628-9b2ea79a-2879-4a95-82ec-d8f7ec94eebf.png">

## 4. DLT (Delta Live Tables)

<img width="406" alt="Screenshot 2023-01-22 at 15 33 42" src="https://user-images.githubusercontent.com/45521680/213929788-f48d70b4-733e-496d-85e5-6577b11d582d.png">
<img width="398" alt="Screenshot 2023-01-22 at 15 34 42" src="https://user-images.githubusercontent.com/45521680/213929785-fd18f375-4fd6-45f5-b7f7-a812a12c83d0.png">

