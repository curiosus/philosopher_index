import requests
from opensearchpy import OpenSearch

# host = "http://focused_gates"
host = "compassionate_khayyam"
port = 9200

def does_index_exist(index_name) -> bool:
    req = f"http://{host}:{port}/{index_name}"
    response = requests.head(req)
    print(f"Response is {response.status_code}")
    return True if response.status_code == 200 else False 
    
def create_index(index_name) -> None:
    client = get_client()
    index_name = "philosophy-index" 
    print(f"Preparing to create index: {index_name}")
    index_body = {
        'settings': {
            'index': {
                'number_of_shards': 4
            }
        }

    }
    response = client.indices.create(index=index_name, body=index_body)
    print(f"Response is: {response}")

def load_index(index_name, document, id)-> None:
    client = get_client()

    response = client.index(
        index = index_name,
        body = document,
        id = id, # TODO this probably shouldn't always be 1
        refresh = True
    )

    print(f"Load Response is: {response}")


def get_client() -> OpenSearch:
    # TODO  return a new client everytime?
    client = OpenSearch(
        hosts = [{'host': "compassionate_khayyam", 'port': port}],
        http_compress = True, 
        use_ssl = False,
        verify_certs = False,
        ssl_assert_hostname = False,
        ssl_show_warn = False
    )
    return client
