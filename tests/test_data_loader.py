import pytest
from src.data_loader import ESDataLoader


def test_es_data_loader_init():
    loader = ESDataLoader(es_url="http://localhost:9200", username="user", password="password", secure=False)
    assert loader.es_url == "http://localhost:9200"
    assert loader.username == "user"
    assert loader.password == "password"
    assert not loader.secure


def test_es_data_loader_connect_missing_url():
    loader = ESDataLoader()
    with pytest.raises(ValueError, match="Elasticsearch URL is missing."):
        loader.connect()


def test_es_data_loader_connect_success(mocker):
    mock_es = mocker.patch("src.data_loader.Elasticsearch")
    mock_instance = mock_es.return_value
    mock_instance.ping.return_value = True

    loader = ESDataLoader(es_url="http://localhost:9200")
    loader.connect()

    assert loader.es_client == mock_instance
    mock_instance.ping.assert_called_once()


def test_es_data_loader_connect_failure(mocker):
    mock_es = mocker.patch("src.data_loader.Elasticsearch")
    mock_instance = mock_es.return_value
    mock_instance.ping.return_value = False

    loader = ESDataLoader(es_url="http://localhost:9200")
    with pytest.raises(ConnectionError, match="Connection failed. Recheck the entered URL and credentials!"):
        loader.connect()


def test_es_data_loader_load_from_cluster(mocker):
    mock_es = mocker.patch("src.data_loader.Elasticsearch")
    mock_instance = mock_es.return_value
    mock_instance.ping.return_value = True
    
    # Mocking properties
    mock_instance.cat.health.return_value = [{"cluster": "test"}]
    mock_instance.nodes.stats.return_value = {"nodes": {}}

    loader = ESDataLoader(es_url="http://localhost:9200")
    data = loader.load_from_cluster()

    assert data['cat_health'] == [{"cluster": "test"}]
    assert data['nodes_stats'] == {"nodes": {}}
    mock_instance.cat.health.assert_called_once()
