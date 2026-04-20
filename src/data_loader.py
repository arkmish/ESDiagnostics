import json
from zipfile import ZipFile
from elasticsearch import Elasticsearch, RequestsHttpConnection


class ESDataLoader:
    def __init__(self, es_url=None, username=None, password=None, secure=False):
        self.es_url = es_url
        self.username = username
        self.password = password
        self.secure = secure
        self.es_client = None

    def connect(self):
        if not self.es_url:
            raise ValueError("Elasticsearch URL is missing.")

        if self.secure:
            self.es_client = Elasticsearch(
                self.es_url,
                connection_class=RequestsHttpConnection,
                http_auth=(self.username, self.password) if self.username and self.password else None,
                verify_certs=False,
                timeout=30
            )
        else:
            self.es_client = Elasticsearch(
                self.es_url,
                connection_class=RequestsHttpConnection,
                verify_certs=False,
                timeout=30
            )

        if not self.es_client.ping():
            raise ConnectionError("Connection failed. Recheck the entered URL and credentials!")

    def load_from_cluster(self):
        self.connect()
        return {
            'cat_health': self.es_client.cat.health(format="json", request_timeout=20),
            'nodes_stats': self.es_client.nodes.stats(format="json", request_timeout=20),
            'cat_nodes': self.es_client.cat.nodes(format="json", request_timeout=20),
            'cat_shards': self.es_client.cat.shards(format="json", request_timeout=20),
            'cluster_health': self.es_client.cluster.health(format="json", request_timeout=20),
            'cat_pending_tasks': self.es_client.cat.pending_tasks(format="json", request_timeout=20),
            'cluster_pending_tasks': self.es_client.cluster.pending_tasks(format="json", request_timeout=20),
            'cat_allocation': self.es_client.cat.allocation(format="json", request_timeout=20),
            'tasks': self.es_client.tasks.list(format="json", request_timeout=20),
            'cat_indices': self.es_client.cat.indices(format="json", request_timeout=20),
            'nodes': self.es_client.nodes.info(format="json", request_timeout=20),
            'cluster_settings': self.es_client.cluster.get_settings(include_defaults=True, request_timeout=20),
            'slow_logs': []  # Assuming API not explicitly supporting slow logs here in the original
        }

    def load_from_zip(self, zip_path):
        unzip = ZipFile(zip_path)
        # Using the original path extraction logic
        file_name = 'tmp/' + zip_path.split("_")[3].split(".")[0] + "/"

        data = {}
        for key in [
            'cat_health', 'cat_allocation', 'cat_nodes', 'cat_shards', 'cat_indices',
            'cat_pending_tasks', 'cluster_pending_tasks', 'tasks', 'cluster_health',
            'cluster_settings', 'nodes', 'nodes_stats'
        ]:
            data[key] = json.load(unzip.open(file_name + f'{key}.txt'))

        with unzip.open(file_name + 'es_search_slowlog.json', 'r') as f:
            response = f.read()
            response = response.replace(b'\n', b'')
            response = response.replace(b'}{', b'},{')
            raw_slow_logs = b"[" + response + b"]"

        data['slow_logs'] = json.loads(raw_slow_logs)
        return data
