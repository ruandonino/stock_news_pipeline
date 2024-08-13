# stock news analyze
This project aims to retrieve news about Brazilian stocks from the Google News API, process the data using an ETL pipeline, and store the results in a Google Cloud Storage data lake. The processed data is also saved in BigQuery for further analysis using SQL.<br>
<br>
The format of data after processing is the following: <br>
![stock_news_data_small](https://github.com/user-attachments/assets/5bdec10a-5950-4f83-bb5d-a5da326074d2)
<br>
This ETL is a dag in Airflow, the following image is the dag of this pipeline: The code of this dag is in the dag_standart file. <br>
<br>
 ![small_dag](https://github.com/user-attachments/assets/705c70fa-b713-4551-8927-052631b85e28)
<br>
 - Checkout_repo - This task is responsible for making a checkout of the repository code to airflow server. After that, the code to be run 
 in dataproc cluster is copied to Google Cloud Storage.<br>
- Copy_to_GCS - The code to run in dataproc cluster is copied to google cloud storage.<br>
- Create_cluster - This task creates a Dataproc cluster configured to run both Pandas and PySpark code. 
 The cluster is provisioned with the necessary libraries and JAR files to ensure seamless execution of the code.<br>
- Api_extract_task - This task uses Google News API to extract data from each stock and save this data in GCS.<br>
- Join_files_task - Here the code joins the parquet file from every stock in just one parquet file the name of date that this code is running.<br>
- Process_task - This task run pyspark code to process the date format of the news, and also to do some filters like delete some empty news.
This code saves the processed parquet file to GCS.<br>
- Delete_cluster - Here the dataproc cluster is deleted.<br>
This ETL pipeline is designed using best practices, to ensure that, this pipeline is idempotent, and that the cluster is up just by the needed time.
This project also has CI/CD automated, with unit tests written using pytest located in the path Tests. The test run in Github Actions using docker container 
with image at path Containers.<br>
Here is the image of processed data in parquet format at GCS, ready to be used by others applications.<br>
![small_gcs](https://github.com/user-attachments/assets/d8fe27fb-8e02-4de1-a12e-d324767385fe)
  <!--imagem das saidas do projeto(parquet no gcs, tabela do bigquery e gráfico com os dados) -->



