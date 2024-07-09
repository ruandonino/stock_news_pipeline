from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat_ws, col
from pyspark.sql.functions import input_file_name, udf
from pyspark.sql.types import StringType, StructType, StructField
import pandas as pd
import re

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("ReadParquetFromGCS") \
    .master("local[*]") \
    .getOrCreate()

stock = 'ABEV3'
list_stocks = [
    ["RRRP3", "3R PETROLEUM"],
    ["ALOS3", "ALLOS"],
    ["ALPA4", "ALPARGATAS"],
    ["ABEV3", "AMBEV"],
    ["ARZZ3", "AREZZO"],
    ["ASAI3", "ASSAI"],
    ["AURE3", "AUREN"],
    ["AZUL4", "AZUL"],
    ["B3SA3", "B3"],
    ["BBSE3", "BBSEGURIDADE"],
    ["BBDC3", "BRADESCO"],
    ["BBDC4", "BRADESCO"],
    ["BRAP4", "BRADESPAR"],
    ["BBAS3", "BANCO DO BRASIL"],
    ["BRKM5", "BRASKEM"],
    ["BRFS3", "BRF"],
    ["BPAC11", "BTGP BANCO"],
    ["CXSE3", "CAIXA SEGURIDADE"],
    ["CRFB3", "CARREFOUR"],
    ["CCRO3", "CCR"],
    ["CMIG4", "CEMIG"],
    ["CIEL3", "CIELO"],
    ["COGN3", "COGNA"],
    ["CSMG3", "COPASA"],
    ["CPLE3", "COPEL"],
    ["CPLE6", "COPEL"],
    ["CSAN3", "COSAN"],
    ["CPFE3", "CPFL"],
    ["CMIN3", "CSN"],
    ["CVCB3", "CVC"],
    ["CYRE3", "CYRELA"],
    ["DXCO3", "DEXCO"],
    ["DIRR3", "DIRECIONAL"],
    ["ECOR3", "ECORODOVIAS"],
    ["ELET3", "ELETROBRAS"],
    ["ELET6", "ELETROBRAS"],
    ["EMBR3", "EMBRAER"],
    ["ENAT3", "ENAUTA"],
    ["ENGI11", "ENERGI"],
    ["ENEV3", "ENEVA"],
    ["EGIE3", "ENGIE"],
    ["EQTL3", "EQUATORIAL"],
    ["EZTC3", "EZTEC"],
    ["FLRY3", "FLEURY"],
    ["GGBR4", "GERDAU"],
    ["GOAU4", "GERDAU"],
    ["GMAT3", "GRUPO MATEUS"],
    ["NTCO3", "GRUPO NATURA"],
    ["SOMA3", "GRUPO SOMA"],
    ["HAPV3", "HAPVIDA"],
    ["HYPE3", "HYPERA"],
    ["IGTI11", "IGUATEMI"],
    ["IRBR3", "IRB BRASIL"],
    ["ITSA4", "ITAUSA"],
    ["ITUB4", "ITAU"],
    ["JBSS3", "JBS"],
    ["KLBN11", "KLABIN"],
    ["RENT3", "LOCALIZA"],
    ["LREN3", "LOJAS RENNER"],
    ["LWSA3", "LOCAWEB"],
    ["MGLU3", "MAGAZINE LUIZA"],
    ["POMO4", "MARCOPOLO"],
    ["MRFG3", "MARFRIG"],
    ["BEEF3", "MINERVA"],
    ["MOVI3", "MOVIDA"],
    ["MRVE3", "MRV"],
    ["MULT3", "MULTIPLAN"],
    ["PCAR3", "PÃO DE ACUCAR"],
    ["PETR3", "PETROBRAS"],
    ["PETR4", "PETROBRAS"],
    ["RECV3", "PETRO RECSA"],
    ["PRIO3", "PETRO RIO"],
    ["PETZ3", "PETZ"],
    ["PSSA3", "PORTO SEGURO"],
    ["RADL3", "RAIA DROGASIL"],
    ["RAIZ4", "RAIZEN"],
    ["RDOR3", "REDE D'OR"],
    ["RAIL3", "RUMO"],
    ["SBSP3", "SABESP"],
    ["SANB11", "SANTANDER"],
]
today = date.today()
# Define the path to the Parquet files in GCS
#parquet_path = r"C:\Users\Ruan Lucas Donino\Downloads\outputs_extracted_data_ABEV3_2024-06-27_ABEV3-2024-06-27"
schema = StructType([
    StructField('title', StringType(), True),
    StructField('date', StringType(), True),
    StructField('link', StringType(), True),
    StructField('img', StringType(), True),
    StructField('media', StringType(), True),
    StructField('reporter', StringType(), True),
    StructField('stock', StringType(), True)
  ])
combined_df = pd.DataFrame()

for stock in list_stocks:
    parquet_path = f"gs://python_files_stock/outputs_extracted_data/{stock[0]}/{today}/{stock[0]}-{today}"
    df_stock = pd.read_parquet(parquet_path)
    df_stock["stock"] = stock[0]
    combined_df = pd.concat([combined_df, df_stock], ignore_index=True)

combined_df.to_parquet(f"gs://python_files_stock/outputs_extracted_data/combined_data/combined_data_{today}")
#combined_df.write.mode("overwrite").parquet(f"gs://python_files_stock/outputs_extracted_data/combined_data/combined_data_{today}")
