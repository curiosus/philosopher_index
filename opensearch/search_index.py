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

index_name = 'python-test-index'

# index_body = {
#   'settings': {
#     'index': {
#       'number_of_shards': 4
#     }
#   }
# }
#response = client.indices.create(index=index_name, body=index_body)

# document = {
#   'title': 'Plato',
#   'text': 'Republic',
#   'year': '-350'
# }

# response = client.index(
#     index = index_name,
#     body = document,
#     id = '1',
#     refresh = True
# )


# print(response)





q = 'plato'
query = {
  'size': 5,
  'query': {
    'multi_match': {
      'query': q,
      # 'fields': ['title^2', 'director']
    }
  }
}

response = client.search(
    body = query,
    index = 'python-test-index'
)

print(response)


print("success")