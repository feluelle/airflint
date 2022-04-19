import textwrap

import pytest
from refactor import Session

from airflint.rules import dag, task, variable


@pytest.mark.parametrize(
    "rules, source, expected",
    [
        (
            dag.EnforceTaskFlowApi,
            """
            from airflow import DAG

            with DAG(dag_id="foo") as dag:
                pass
            """,
            """
            from airflow import DAG
            from airflow.decorators import dag

            @dag(dag_id="foo")
            def foo():
                pass
            foo()
            """,
        ),
        (
            dag.EnforceStaticStartDate,
            """
            from airflow import DAG
            from airflow.utils.dates import days_ago

            with DAG(dag_id="foo", start_date=days_ago(1), schedule_interval="@daily") as dag:
                pass
            """,
            """
            from airflow import DAG
            from airflow.utils.dates import days_ago
            from pendulum import datetime

            with DAG(dag_id="foo", start_date=datetime(2022, 4, 18), schedule_interval="@daily") as dag:
                pass
            """,
        ),
        (
            task.EnforceTaskFlowApi,
            """
            from airflow.operators.python import PythonOperator

            def foo():
                pass

            PythonOperator(task_id="foo", python_callable=foo)
            """,
            """
            from airflow.operators.python import PythonOperator
            from airflow.decorators import task

            @task(task_id="foo")
            def foo():
                pass

            foo()
            """,
        ),
        (
            task.EnforceTaskFlowApi,
            """
            from airflow.operators.python import PythonVirtualenvOperator

            def foo():
                pass

            PythonVirtualenvOperator(task_id="foo", python_callable=foo)
            """,
            """
            from airflow.operators.python import PythonVirtualenvOperator
            from airflow.decorators import task

            @task.virtualenv(task_id="foo")
            def foo():
                pass

            foo()
            """,
        ),
        (
            task.EnforceTaskFlowApi,
            """
            from airflow.operators.python import PythonOperator

            def foo():
                pass

            task_foo = PythonOperator(task_id="foo", python_callable=foo)
            """,
            """
            from airflow.operators.python import PythonOperator
            from airflow.decorators import task

            @task(task_id="foo")
            def foo():
                pass

            task_foo = foo()
            """,
        ),
        (
            task.EnforceTaskFlowApi,
            """
            from airflow.operators.python import PythonOperator

            def foo(fizz, bar):
                pass

            task_foo = PythonOperator(task_id="foo", python_callable=foo, op_kwargs=dict(bar="bar"), op_args=["fizz"])
            """,
            """
            from airflow.operators.python import PythonOperator
            from airflow.decorators import task

            @task(task_id="foo")
            def foo(fizz, bar):
                pass

            task_foo = foo("fizz", bar="bar")
            """,
        ),
        (
            [variable.ReplaceVariableGetByJinja],
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            BashOperator(task_id="foo", bash_command=Variable.get("FOO"))
            """,
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            BashOperator(task_id="foo", bash_command='{{ var.value.FOO }}')
            """,
        ),
    ],
)
def test_rules(rules, source, expected):
    source = textwrap.dedent(source)
    expected = textwrap.dedent(expected)

    session = Session(rules=rules)
    assert session.run(source) == expected
