import pytest
from unittest.mock import patch
from GoogleNews import GoogleNews

# Import the function to be tested
from pyspark_extract import readNewsDate

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

# Run the tests
if __name__ == "__main__":
    pytest.main()