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

index_name = 'philosophy-index'

query = {
  "query": {
      "match": {
          "text": "Pythias"
      }
  }    

}

response = client.search(
    body = query,
    index = index_name 
)

hit_data = response["hits"]
hits = hit_data["hits"]
for hit in hits:
    source = hit['_source']
    title = source['title']
    print(title)


