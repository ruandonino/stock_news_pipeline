from datetime import date, timedelta

import pytest
from unittest.mock import patch, MagicMock
from GoogleNews import GoogleNews
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import Row

import sys
import os

# Ensure the parent directory is in the system path
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the function to be tested
from Code_ETL.pyspark_extract import readNewsDate, listNewsToDataPandas
from Code_ETL.join_by_data import join_dataframes
from Code_ETL.process_data import process_data


# Mock data to be returned by GoogleNews
mock_results = [
    {'title': 'Test Title 1', 'desc': 'None', 'datetime': "nan", 'link': "news.google.com", 'img':"news.google.com/api",
     "media":"Expert XP", "site":"news.google.com", "reporter": None},
    {'title': 'Test Title 2', 'desc': 'None', 'datetime': "nan", 'link': "news.google.com", 'img':"news.google.com/api",
     "media":"Expert XP", "site":"news.google.com", "reporter": None},
    {'title': 'Test Title 3', 'desc': 'None', 'datetime': "nan", 'link': "news.google.com", 'img':"news.google.com/api",
     "media":"Expert XP", "site":"news.google.com", "reporter": None},
]


@pytest.fixture
def mock_googlenews(mocker):
    mocker.patch.object(GoogleNews, 'results', return_value=mock_results)
    return GoogleNews

def test_readNewsDate(mock_googlenews):
    term_test = 'WEGE3'
    # Call the function
    results = readNewsDate(term=term_test)

    assert isinstance(results, list)
    for result in results:
        assert isinstance(result, dict)
        assert 'title' in result
        assert 'desc' in result
        assert 'datetime' in result
        assert 'link' in result
        assert 'img' in result
        assert 'media' in result
        assert 'site' in result
        assert 'reporter' in result
        assert len(result) == 8

def test_listNewsToDataPandas():
    input_data = [
        {
            'title': 'Title 1',
            'date': '2024-07-12',
            'link': 'http://example.com/1',
            'img': 'http://example.com/img1.jpg',
            'media': 'Media 1',
            'reporter': 'Reporter 1',
            'desc': 'Description 1',
            'datetime': '2024-07-12T12:00:00',
            'site': 'Example Site 1'
        },
        {
            'title': 'Title 2',
            'date': '2024-07-13',
            'link': 'http://example.com/2',
            'img': 'http://example.com/img2.jpg',
            'media': 'Media 2',
            'reporter': 'Reporter 2',
            'desc': 'Description 2',
            'datetime': '2024-07-13T12:00:00',
            'site': 'Example Site 2'
        }
    ]

    expected_data = {
        'title': ['Title 1', 'Title 2'],
        'date': ['2024-07-12', '2024-07-13'],
        'link': ['http://example.com/1', 'http://example.com/2'],
        'img': ['http://example.com/img1.jpg', 'http://example.com/img2.jpg'],
        'media': ['Media 1', 'Media 2'],
        'reporter': ['Reporter 1', 'Reporter 2']
    }
    expected_df = pd.DataFrame(expected_data)

    result_df = listNewsToDataPandas(input_data)

    pd.testing.assert_frame_equal(result_df, expected_df)

def test_join_dataframes():
    # Mock data to be returned by pd.read_parquet
    mock_data_1 = pd.DataFrame({
        'col1': [1, 2],
        'col2': [3, 4]
    })

    mock_data_2 = pd.DataFrame({
        'col1': [5, 6],
        'col2': [7, 8]
    })

    @patch('pandas.read_parquet')
    @patch('datetime.date.today')
    def test_join_dataframes(mock_today, mock_read_parquet):
        mock_today.return_value = date(2024, 7, 12)
        mock_read_parquet.side_effect = [mock_data_1, mock_data_2]

        input_data = [
            'WEGE3',
            'BBAS3'
        ]

        expected_data = {
            'col1': [1, 2, 5, 6],
            'col2': [3, 4, 7, 8]
        }
        expected_df = pd.DataFrame(expected_data)

        result_df = join_dataframes(input_data)

        pd.testing.assert_frame_equal(result_df, expected_df)
        mock_read_parquet.assert_any_call(
            'gs://python_files_stock/outputs_extracted_data/WEGE3/2024-07-12/WEGE3-2024-07-12')
        mock_read_parquet.assert_any_call(
            'gs://python_files_stock/outputs_extracted_data/BBAS3/2024-07-12/BBAS3-2024-07-12')

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("TestSession") \
        .master("local[*]") \
        .config("spark.python.worker.retries", "10") \
        .getOrCreate()

@patch('process_data.SparkSession')
@patch('process_data.date', spec=date)
def test_process_data(mock_date, mock_spark, spark):
    # Mock the date to return a consistent value
    mock_date.today.return_value = date(2023, 7, 11)
    mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)

    # Create a mock SparkSession
    mock_spark.builder.appName.return_value.config.return_value.getOrCreate.return_value = spark

    # Create a sample DataFrame to mimic reading from GCS
    sample_data = [
        Row(title="  Some Title  ", date="9 of Dec. of 2023", link="http://example.com", img="http://example.com/image.jpg", media="Media1", reporter="Reporter1"),
        Row(title="  Another Title  ", date="10 dias atrás", link="http://example.com", img="http://example.com/image.jpg", media="Media2", reporter="Reporter2"),
        Row(title="", date="Ontem", link="http://example.com", img="http://example.com/image.jpg", media="Media3", reporter="Reporter3"),
    ]
    df = spark.createDataFrame(sample_data)

    # Mock the read.parquet method to return the sample DataFrame
    with patch.object(spark.read, 'parquet', return_value=df):
        # Mock the DataFrame write mode and parquet method
        with patch.object(df.write, 'mode') as mock_write_mode:
            mock_write = MagicMock()
            mock_write_mode.return_value = mock_write
            # Mock the Spark configurations related to Google Cloud Storage
            with patch('process_data.SparkSession.builder.config') as mock_config:
                mock_config.return_value = mock_config
                mock_config.getOrCreate.return_value = spark
                # Call the process_data function
                process_data()
                # Verify the transformations
                result = df.collect()
                assert result[0].title.strip() == "Some Title"
                assert result[0].Formatted_Date == "09/12/2023"
                assert result[1].Formatted_Date == (date(2023, 7, 11) - timedelta(days=10)).strftime("%d/%m/%Y")
                assert result[2].Formatted_Date == (date(2023, 7, 10)).strftime("%d/%m/%Y")

                # Check if the parquet file was read from the correct path
                read_file_path = "gs://python_files_stock/outputs_extracted_data/combined_data/combined_data_2023-07-11"
                spark.read.parquet.assert_any_call(read_file_path)

                # Check if the write method was called with the correct path
                output_path = "gs://python_files_stock/outputs_processed_data/processed_data_2023-07-11"
                mock_write.parquet.assert_any_call(output_path)
# Run the tests
if __name__ == "__main__":
    pytest.main()