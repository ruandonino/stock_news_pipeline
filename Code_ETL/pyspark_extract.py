from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import date
import time
import pandas as pd
from GoogleNews import GoogleNews


def readNewsDate(term: str, language:str ='pt', region:str ='BR', pag_limit:int=10):
  googlenews = GoogleNews(lang=language, region=region)
  googlenews.get_news(term)
  googlenews.set_time_range('01/01/2023','25/06/2024')
  #googlenews.get_page(pag_limit)
  #print(googlenews.get_texts())
  print(googlenews.results())
  return googlenews.results()

def listNewsToDataSpark(list_data:list, spark_session):
  main_list=[]
  del_columns = ['desc', 'datetime', 'site']
  for dict_data in list_data:
    list_element = [v for k, v in dict_data.items() if k not in del_columns]
    main_list.append(list_element)

  columns = ['title', 'date', 'link', 'img', 'media', 'reporter']
  schema = StructType([
    StructField('title', StringType(), True),
    StructField('date', StringType(), True),
    StructField('link', StringType(), True),
    StructField('img', StringType(), True),
    StructField('media', StringType(), True),
    StructField('reporter', StringType(), True)
  ])
  df_data = spark_session.createDataFrame(main_list, schema)
  return df_data


def listNewsToDataPandas(list_data:list):
  main_list=[]
  del_columns = ['desc', 'datetime', 'site']
  for dict_data in list_data:
    list_element = [v for k, v in dict_data.items() if k not in del_columns]
    main_list.append(list_element)

  columns = ['title', 'date', 'link', 'img', 'media', 'reporter']
  schema = StructType([
    StructField('title', StringType(), True),
    StructField('date', StringType(), True),
    StructField('link', StringType(), True),
    StructField('img', StringType(), True),
    StructField('media', StringType(), True),
    StructField('reporter', StringType(), True)
  ])

  df_data = pd.DataFrame(main_list, columns=columns)
  return df_data

def saveDataframeToParquet(df_data, output_path):
  df_data.write.mode("overwrite").parquet(output_path)
  print(f"Data saved to {output_path}")

if __name__ == "__main__":
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
    # Create a Spark session
    #spark = SparkSession.builder.appName("API to Parquet").getOrCreate()
    index_initial=0

    for stock in list_stocks:
      print(stock[0])
      output_extrated_data_path = f"gs://python_files_stock/outputs_extracted_data/{stock[0]}/{today}/{stock[0]}-{today}"
      list_news = readNewsDate(term =stock[0])
      df_news = listNewsToDataPandas(list_news)
      df_news.to_parquet(output_extrated_data_path)
      #df_news = listNewsToDataSpark(list_news, spark)
      #saveDataframeToParquet(df_news, output_extrated_data_path)
      #df_news.unpersist()
