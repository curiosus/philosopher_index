from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from bs4 import BeautifulSoup


def fetch_philo_page(**context):
    conf = context['dag_run'].conf
    print(f"Received conf: {conf}")
    url = context['dag_run'].conf.get('url')
    response = requests.get(url)
    response.raise_for_status()
    html_content = response.text
    return html_content

def parse_philo_page(**context):
    html_content = context['ti'].xcom_pull(task_ids='fetch_philo_page')
    soup = BeautifulSoup(html_content, "html.parser")
    title = soup.title.string
    paragraphs = soup.select("div.mw-parser-output > p")
    text_content = "\n".join(p.get_text(separator=" ", strip=True) for p in paragraphs if p.get_text(strip=True))
    return {"title": title, "text_content": text_content}

def index_philo_page(**context):
    data = context['ti'].xcom_pull(task_ids='parse_philo_page')
    title = data["title"]
    text_content = data["text_content"]
    # what to do here
    # the next step is to index this with open search
    # after that to use spark to index multiple content types in parallel

    url = "http://localhost:9200/philo-index" 


# curl -XPUT "http://localhost:9200/hotels-index" -H 'Content-Type: application/json' -d'
    data = {
                "settings": {
                    "index.knn": True
                },
                "mappings": {
                    "properties": {
                        "title": {"type": "text"},
                        "text_content": {"type": "text"},
                        "embedding": {
                            "type": "knn_vector",
                            "dimension": 384,
                            "space_type": "consinesimil"
                        }
                    }
                }
            }


    response = requests.put(url, json=data)
    print(f"Status Code: {response.status_code}")




    return {"title": title, "text_content": text_content}


with DAG(
    dag_id="philo_index",
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    tags=["philo"]
) as dag:

    get_page = PythonOperator(
        task_id="fetch_philo_page",
        python_callable=fetch_philo_page,
        provide_context=True,
    )

    parse_page = PythonOperator(
        task_id="parse_philo_page",
        python_callable=parse_philo_page,
        provide_context=True,
    )

    index_page = PythonOperator(
        task_id="index_philo_page",
        python_callable=index_philo_page,
        provide_context=True,
    )

    get_page >> parse_page >> index_page
