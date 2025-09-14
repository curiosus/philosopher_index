from opensearchpy import OpenSearch
from opensearch_dsl import Search


host = 'localhost'
port = 9200

# Create the client with SSL/TLS and hostname verification disabled.
client = OpenSearch(
    hosts = [{'host': host, 'port': port}],
    http_compress = True, # enables gzip compression for request bodies
    use_ssl = False,
    verify_certs = False,
    ssl_assert_hostname = False,
    ssl_show_warn = False
)

index_name = "philosophy-index" 
index_body = {
  'settings': {
    'index': {
      'number_of_shards': 4
    }
  }
}
response = client.indices.create(index=index_name, body=index_body)
print(response)

print("success")