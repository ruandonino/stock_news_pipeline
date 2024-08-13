# stock news analyze
 This project seek to get news from google news API about Brazilians stocks, process this data using an ETL pipeline and 
 save this data in Google Cloud Storage datalake and in Bigquery to be analyzed using SQL.

The format of data after processing is the following: <br>
<br>
  ![stock_news_data_small](https://github.com/user-attachments/assets/837e4655-a655-4265-bdc6-b8be7c2a825d)

This ETL is a dag in Airflow, the following image is the dag of this pipeline: The code of this dag is in the dag_standart file. <br>
 <br>
 ![small_dag](https://github.com/user-attachments/assets/a3c08b54-7e86-4d25-9a9b-0b5ef981152c)
<br>
 - Checkout_repo - This task is responsible for make a checkout of the repository code to airflow server. After it, the code to be run 
 in dataproc cluster is copied to google clous storage.<br>
<br>
- Copy_to_GCS - The code to run in dataproc cluster is copied to google cloud storage.<br>
<br>
- Create_cluster - This task creates a Dataproc cluster configured to run both Pandas and PySpark code. 
 The cluster is provisioned with the necessary libraries and JAR files to ensure seamless execution of the code.<br>
<br>
- Api_extract_task - This task use Google News API to extract data from each stock and save this data in GCS.<br>
<br>
- Join_files_task - Here the code join the parquet file from every stock in just one parquet file the name of date that this code is running.<br>
<br>
- Process_task - This task run pyspark code to process the date format of the news, and also to do some filters like delete some empty news.
This code saves the processed parquet file to GCS.<br>
<br>
- Delete_cluster - Here the dataproc cluster is deleted.<br>
<br>
This ETL pipeline is designed using best practices, ensure that this pipeline be idempotent, and that the cluster is up just by the needed time.
This project also have CI/CD automated, with unit tests writed using pytest located in the path Tests. The test run in Github Actions using docker container 
with image at path Containers.<br>
<br>
![dag_small](https://github.com/user-attachments/assets/64cbfdda-af4f-4464-ba89-42f608b26a31)<br>
<br>
Here is the image of processed data in parquet format at GCS, ready to be used by others applications.<br>
<br>
![small_gcs](https://github.com/user-attachments/assets/d8fe27fb-8e02-4de1-a12e-d324767385fe)
  <!--imagem das saidas do projeto(parquet no gcs, tabela do bigquery e gráfico com os dados) -->



