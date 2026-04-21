import pytest
from airflow.models import DagBag

def test_dag_loaded():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert "integrated_dag" in dagbag.dags
    assert len(dagbag.import_errors) == 0

def test_dag_has_correct_tasks():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dagbag.dags["integrated_dag"]
    task_ids = [task.task_id for task in dag.tasks]
    assert "trigger_airbyte" in task_ids
    assert "wait_for_airbyte" in task_ids
    assert "dbt_run" in task_ids
    assert "dbt_test" in task_ids
    assert len(task_ids) == 4

def test_dag_task_dependencies():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dagbag.dags["integrated_dag"]

    trigger = dag.get_task("trigger_airbyte")
    wait = dag.get_task("wait_for_airbyte")
    dbt_run = dag.get_task("dbt_run")
    dbt_test = dag.get_task("dbt_test")

    assert wait.task_id in [t.task_id for t in trigger.downstream_list]
    assert dbt_run.task_id in [t.task_id for t in wait.downstream_list]
    assert dbt_test.task_id in [t.task_id for t in dbt_run.downstream_list]

def test_dag_schedule():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dagbag.dags["integrated_dag"]
    assert str(dag.schedule_interval) == "0 6 * * *"

def test_dag_tags():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dagbag.dags["integrated_dag"]
    assert "airbyte" in dag.tags
    assert "dbt" in dag.tags
    assert "integrated" in dag.tags

def test_dag_default_args():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dagbag.dags["integrated_dag"]
    assert dag.default_args["owner"] == "vicky"
    assert dag.default_args["retries"] == 1
