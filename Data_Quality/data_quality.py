from pyspark.sql import SparkSession
import great_expectations as gx
from great_expectations.checkpoint import Checkpoint
from great_expectations import expectations as gxe
from datetime import date


def read_df(spark, read_path):
    df_stock = spark.read.parquet(read_path)
    return df_stock

def setup_context(context_path):
    context_out = gx.get_context(context_root_dir=context_path)
    return context_out

def create_data_source(data_source_name, data_asset_name, context):
    data_source = context.data_sources.add_or_update_spark(
        name= data_source_name,
    )
    data_asset = data_source.add_dataframe_asset(name=data_asset_name)
    return context


def create_expectations(context, expectations_suite_name):
    suite = gx.ExpectationSuite(name=expectations_suite_name)

    suite = context.suites.add(suite)
    expectation_not_null_title = gx.expectations.ExpectColumnValuesToNotBeNull(column="title")
    suite.add_expectation(expectation_not_null_title)
    expectation_not_null_date = gx.expectations.ExpectColumnValuesToNotBeNull(column="date")
    suite.add_expectation(expectation_not_null_date)
    expectation_not_null_link = gx.expectations.ExpectColumnValuesToNotBeNull(column="link")
    suite.add_expectation(expectation_not_null_link)
    expectation_not_null_media = gx.expectations.ExpectColumnValuesToNotBeNull(column="media")
    suite.add_expectation(expectation_not_null_media)

    expectation_regex_link = gx.expectations.ExpectColumnValuesToMatchRegex(column="link", regex=r"https://")
    suite.add_expectation(expectation_regex_link)
    expectation_regex_image = gx.expectations.ExpectColumnValuesToMatchRegex(column="image", regex=r"https://")
    suite.add_expectation(expectation_regex_image)
    expectation_regex_title = gx.expectations.ExpectColumnValuesToMatchRegex(column="title", regex="\b\w+\b")
    suite.add_expectation(expectation_regex_title)
    expectation_regex_date = gx.expectations.ExpectColumnValuesToMatchRegex(column="date", regex="\d{1,2}/\d{1,2}/\d{4}")
    suite.add_expectation(expectation_regex_date)

    return context


def create_validator(context, batch_definition, expectation_suite,validator_name):

    validation_definition = gx.ValidationDefinition(
        data=batch_definition, suite=expectation_suite, name=validator_name
    )
    validation_definition = context.validation_definitions.add(validation_definition)

    return context


def create_checkpoint(context, validation_definition, checkpoint_name):
    action_list = [
        gx.checkpoint.UpdateDataDocsAction(
            name="update_all_data_docs",
        )
    ]

    checkpoint = gx.Checkpoint(
        name=checkpoint_name,
        validation_definitions=validation_definition,
        actions=action_list,
        result_format={"result_format": "COMPLETE"},
    )
    context.checkpoints.add(checkpoint)

    return context


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("DataQuality") \
        .getOrCreate()

    today = date.today()
    read_path = f"gs://python_files_stock2/outputs_processed_data/processed_data_{today}"
    df_stock = read_df(spark, read_path)
    context_path = r".\context"
    context = setup_context(context_path)
    data_source_name = "stock_data_in_memory"
    data_asset_name = "stock_data_asset"
    context = create_data_source(data_source_name, data_asset_name, context)
    data_asset = context.data_sources.get(data_source_name).get_asset(data_asset_name)
    batch_definition_name = "stock_data_batch"
    batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)
    batch_parameters = {"dataframe": df_stock}
    expectation_suite_name = "data_quality_expectations"
    context = create_expectations(context, expectation_suite_name)
    validator_name = "data_quality_validator"
    expectation_suite = context.suites.get(name=expectation_suite_name)
    context = create_validator(context, batch_definition, expectation_suite, validator_name)
    validation_definition = context.validation_definitions.get(validator_name)
    checkpoint_name = "data_quality_checkpoint"
    context = create_checkpoint(context, validation_definition, checkpoint_name)
    base_directory = f"gs://python_files_stock2/outputs_extracted_data/quality_data_{today}"
    site_config = {
        "class_name": "SiteBuilder",
        "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
        "store_backend": {
            "class_name": "TupleFilesystemStoreBackend",
            "base_directory": base_directory,
        },
    }
    site_name = "my_data_docs_site"
    context.add_data_docs_site(site_name=site_name, site_config=site_config)
    checkpoint = context.checkpoints.get(checkpoint_name)
    validation_results = checkpoint.run(
        batch_parameters=batch_parameters
    )


