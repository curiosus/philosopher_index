from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from opensearch_utils import does_index_exist, create_index, load_index


def fetch_philo_page(**context):
    conf = context['dag_run'].conf.get('url')
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
    article = soup.find("div", id="article-content")
    paragraphs = article.find_all("p")
    # paragraphs = soup.select("div.mw-parser-output > p")
    # text_content = "\n".join(p.get_text(separator=" ", strip=True) for p in paragraphs if p.get_text(strip=True))
    text_content = "\n\n".join(p.get_text(strip=True) for p in paragraphs)
    print(f"BONG HIT text_content: {text_content}")
    return {"title": title, "text_content": text_content}

def index_philo_page(**context):
    data = context['ti'].xcom_pull(task_ids='parse_philo_page')
    title = data["title"]
    text_content = data["text_content"]

    # what to do here
    # the next step is to index this with open search
    # after that to use spark to index multiple content types in parallel


    #does index already exist?
    index_name = "philosophy-index"
    index_exists = does_index_exist(index_name) 
    print(f"Index exists: {index_exists}")
    #if not exists then create index
    if not index_exists:
        create_index(index_name=index_name)
    
    #now we need to add or update the index with 
    #the title and text_content
    doc = {
        'title': title,
        'text': text_content,
    }
    load_index(index_name=index_name, document=doc, id=title)

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
