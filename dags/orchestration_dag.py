from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import DagRun, XCom
from airflow.settings import Session
from airflow.utils.dates import days_ago

with DAG(
    dag_id="orchestration_dag",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    tags=["philo"]
) as dag:

    parent_run_id = f"orchestration_{days_ago(0).isoformat()}"

    trigger_plato = TriggerDagRunOperator(
        task_id="trigger_plato",
        trigger_dag_id="philo_index",
        wait_for_completion=True,
        reset_dag_run=True,
        conf={"url": "https://en.wikipedia.org/wiki/Plato", "parent_run_id": parent_run_id},
    )

    trigger_aristotle = TriggerDagRunOperator(
        task_id="trigger_aristotle",
        trigger_dag_id="philo_index",
        wait_for_completion=True,
        reset_dag_run=True,
        conf={"url": "https://en.wikipedia.org/wiki/Aristotle", "parent_run_id": parent_run_id},
    )


    trigger_descartes = TriggerDagRunOperator(
        task_id="trigger_descartes",
        trigger_dag_id="philo_index",
        wait_for_completion=True,
        reset_dag_run=True,
        conf={"url": "https://en.wikipedia.org/wiki/René_Descartes", "parent_run_id": parent_run_id},
    )

    @task
    def collect_results(**context):
        session = Session()


        dag_runs = (
            session.query(DagRun)
            .filter(DagRun.dag_id == "philo_index")
            .order_by(DagRun.execution_date.desc())
            .limit(3)
            .all()
        )

        results = []
        names = ["Descartes", "Aristotle", "Plato"]

        for i, dag_run in enumerate(dag_runs):
            value = XCom.get_one(
                execution_date=dag_run.execution_date,
                task_id="index_philo_page",
                dag_id="philo_index",
                key="return_value",
                session=session,
            )

            if value:
                results.append((names[i], value))

        print("Collected Results:")
        for name, result in results:
            print(f"{name}: {result}")

        session.close()



    collect_result = collect_results()

    trigger_plato >> trigger_aristotle >> trigger_descartes >> collect_result
