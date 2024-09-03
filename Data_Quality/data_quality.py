from pyspark.sql import SparkSession
import great_expectations as gx
from great_expectations.checkpoint import Checkpoint
from datetime import date


def read_df(spark, read_path):
    df_stock = spark.read.parquet(read_path)
    return df_stock

def setup_context(context_path):
    context_out = gx.get_context(context_root_dir=context_path)
    return context_out

def create_data_source(df_stock, context):
    data_source = context.data_sources.add_or_update_spark(
        name="stock_data_in_memory",
    )
    data_asset = data_source.add_dataframe_asset(name="stock_data")
    return data_source,data_asset

def create_expectations(context, batch_request,expectations_suite_name):
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectations_suite_name,
    )
    validator.expect_column_to_not_be_null(column="title")
    validator.expect_column_to_not_be_null(column="date")
    validator.expect_column_to_not_be_null(column="link")
    validator.expect_column_to_not_be_null(column="media")

    validator.expect_column_values_to_match_regex(column="link", regex=r"https://")
    validator.expect_column_values_to_match_regex(column="image", regex=r"https://")
    validator.expect_column_values_to_match_regex(column="title", regex="\b\w+\b")
    validator.expect_column_values_to_match_regex(column="date", regex="\d{1,2}/\d{1,2}/\d{4}")

    return context, expectations_suite_name

def create_checkpoint(context, expectations_suite_name, checkpoint_name, batch_request):
    checkpoint = Checkpoint(
        name=checkpoint_name,
        run_name_template=checkpoint_name + "_template",
        data_context=context,
        batch_request=batch_request,
        expectation_suite_name=expectations_suite_name,
        action_list = [
            {
                "name": "store_validation_result",
                "action": {
                    "class_name": "StoreValidationResultAction"
                },
            },
            {
                "name": "update_data_docs",
                "action": {
                    "class_name": "UpdateDataDocsAction"
                },
            }
        ],
    )
    return checkpoint


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("DataQuality") \
        .getOrCreate()

    today = date.today()
    read_path = f"gs://python_files_stock2/outputs_processed_data/processed_data_{today}"
    df_stock = read_df(spark, read_path)
    context_path = r".\context"
    context = setup_context(context_path)
    data_source, data_asset = create_data_source(df_stock, context)
    batch_request = data_asset.build_batch_request(dataframe=df_stock)
    expectations_suite_name = "data_quality_expectations"
    context, expectations_suite_name = create_expectations(context, batch_request, expectations_suite_name)
    checkpoint_name = "data_quality_checkpoint"
    checkpoint = create_checkpoint(context, expectations_suite_name, checkpoint_name, batch_request)
    context.add_or_update_checkpoint(checkpoint=checkpoint)
    checkpoint_result = checkpoint.run()
    context.build_data_docs()


