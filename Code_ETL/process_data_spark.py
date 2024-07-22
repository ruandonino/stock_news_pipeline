from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when
from datetime import date
from pyspark.sql.functions import regexp_extract, current_date, date_format, expr, year


def read_df(spark, read_path):
    # Read the Parquet file into a DataFrame
    df_data_stock = spark.read.parquet(read_path)
    return df_data_stock
def transform_df(df_data_stock):
    # Specify the column to check for empty cells and the column to trim
    column_to_check = "title"  # Column to check for empty cells

    # Drop rows where the specified column is empty
    df_data_stock = df_data_stock.filter(col(column_to_check).isNotNull() & (col(column_to_check) != ""))

    # Trim whitespace from the start and end of cells in the specified string column
    columns_dataframe = ["title", "date", "link", "img", "media", "reporter"]
    for column in columns_dataframe:
        df_data_stock = df_data_stock.withColumn(column, trim(col(column)))
        df_data_stock = df_data_stock.withColumn(column, when(col(column).isNull(), "").otherwise(col(column)))

    # Define month abbreviations mapping as an SQL case statement
    month_case_statement = """
            CASE
                WHEN month = 'Jan.' THEN '01'
                WHEN month = 'Fev.' THEN '02'
                WHEN month = 'Mar.' THEN '03'
                WHEN month = 'Abr.' THEN '04'
                WHEN month = 'Mai.' THEN '05'
                WHEN month = 'Jun.' THEN '06'
                WHEN month = 'Jul.' THEN '07'
                WHEN month = 'Ago.' THEN '08'
                WHEN month = 'Set.' THEN '09'
                WHEN month = 'Out.' THEN '10'
                WHEN month = 'Nov.' THEN '11'
                WHEN month = 'Dez.' THEN '12'
            END
        """

    # Extract parts of the date for "9 of Dec. of 2023"
    df_data_stock = df_data_stock.withColumn("Day", regexp_extract(col("date"), r"(\d{1,2}) de \w+\. de \d*", 1)) \
        .withColumn("Month", regexp_extract(col("date"), r"\d{1,2} de (\w+)\. de \d*", 1)) \
        .withColumn("Year", regexp_extract(col("date"), r"\d{1,2} de \w+\. de (\d*)", 1))
    df_data_stock = df_data_stock.withColumn("Year",
                                             when(col("Year") == "", year(current_date())).otherwise(col("Year")))
    print("first step")
    print(df_data_stock.collect())
    # Convert the extracted month to a two-digit format
    df_data_stock = df_data_stock.withColumn("Month_Num", expr(month_case_statement).alias("month_num"))

    # Construct the full date from the extracted parts
    df_data_stock = df_data_stock.withColumn("Full_Date", expr("concat(Year, '-', Month_Num, '-', lpad(Day, 2, '0'))"))

    # Extract the number of days ago from the 'Date' column and handle "yesterday"
    df_data_stock = df_data_stock.withColumn("Days_Ago", when(col("date") == "Ontem", 1)
                                             .otherwise(
        regexp_extract(col("date"), r"(\d+) dias atrás", 1).cast("int")))

    # Calculate the actual date by subtracting the days ago from the current date or using the full date
    df_data_stock = df_data_stock.withColumn("Actual_Date", when(col("Days_Ago").isNotNull(),
                                                                 expr("date_sub(current_date(), Days_Ago)"))
                                             .otherwise(col("Full_Date")))
    print("second step")
    print(df_data_stock.collect())

    # Format the actual date to 'dd/MM/yyyy'
    df_data_stock = df_data_stock.withColumn("Formatted_Date", date_format(col("Actual_Date"), "dd/MM/yyyy"))

    # Drop intermediate columns if not needed
    df_data_stock = df_data_stock.drop("Date", "Days_Ago", "Day", "Month", "Year", "Month_Num", "Full_Date",
                                       "Actual_Date")
    return df_data_stock
def process_data_spark():
    # Initialize a SparkSession
    spark = SparkSession.builder \
        .appName("DropEmptyAndTrimWhitespace") \
        .getOrCreate()

    today = date.today()
    # Define the path to the Parquet file on Google Cloud Storage
    read_file_path = f"gs://python_files_stock/outputs_extracted_data/combined_data/combined_data_{today}"
    # Read the Parquet file into a DataFrame
    df_data_stock = read_df(spark,read_file_path)

    # Transform the DataFrame
    df_data_stock = transform_df(df_data_stock)

    output_path = f"gs://python_files_stock/outputs_processed_data/processed_data_{today}"
    df_data_stock.write.mode('overwrite').parquet(output_path)
    return df_data_stock




if __name__ == "__main__":
    result_data =process_data_spark()