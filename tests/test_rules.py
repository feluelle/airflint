import textwrap

import pytest
from refactor import Session

from airflint.rules.use_function_level_imports import UseFunctionLevelImports
from airflint.rules.use_jinja_variable_get import UseJinjaVariableGet


@pytest.mark.parametrize(
    "rule, source, expected",
    [
        pytest.param(
            UseFunctionLevelImports,
            """
            from functools import reduce
            from operator import add
            import dataclass

            def something():
                a = reduce(x, y)
                b = add(a, a)
                return b

            def other_thing():
                return dataclass(something(1, 2))
            """,
            """
            from functools import reduce
            from operator import add
            import dataclass

            def something():
                from functools import reduce
                from operator import add
                a = reduce(x, y)
                b = add(a, a)
                return b

            def other_thing():
                import dataclass
                return dataclass(something(1, 2))
            """,
            id="UseFunctionLevelImports | SUCCESS | general",
        ),
        pytest.param(
            UseFunctionLevelImports,
            """
            from airflow.decorators import dag, task
            from operator import add

            @dag()
            def my_dag():
                @task()
                def my_task():
                    add(1, 2)
            """,
            """
            from airflow.decorators import dag, task
            from operator import add

            @dag()
            def my_dag():
                @task()
                def my_task():
                    from operator import add
                    add(1, 2)
            """,
            id="UseFunctionLevelImports | SKIP | dag decorator",
        ),
        pytest.param(
            UseFunctionLevelImports,
            """
            from airflow.decorators import dag, task
            from operator import add

            @dag()
            def my_dag():
                @task()
                def my_task():
                    add(1, 2)
                    add(1, 2)
            """,
            """
            from airflow.decorators import dag, task
            from operator import add

            @dag()
            def my_dag():
                @task()
                def my_task():
                    from operator import add
                    add(1, 2)
                    add(1, 2)
            """,
            id="UseFunctionLevelImports | SUCCESS | unique",
        ),
        pytest.param(
            UseFunctionLevelImports,
            """
            import random
            from airflow import DAG
            from airflow.decorators import task

            with DAG() as dag:
                @task.branch()
                def random_choice():
                    return random.choice(['task_1', 'task_2'])
            """,
            """
            import random
            from airflow import DAG
            from airflow.decorators import task

            with DAG() as dag:
                @task.branch()
                def random_choice():
                    import random
                    return random.choice(['task_1', 'task_2'])
            """,
            id="UseFunctionLevelImports | SKIP | function decorators",
        ),
        pytest.param(
            UseJinjaVariableGet,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            FakeOperator(task_id="fake", foo=Variable.get("FOO"))
            """,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            FakeOperator(task_id="fake", foo='{{ var.value.FOO }}')
            """,
            id="UseJinjaVariableGet | SUCCESS | direct assignment with key",
        ),
        pytest.param(
            UseJinjaVariableGet,
            """
            from airflow.models import Variable
            from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

            KubernetesPodOperator(task_id="fake", image=Variable.get("FOO"))
            """,
            """
            from airflow.models import Variable
            from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

            KubernetesPodOperator(task_id="fake", image=Variable.get("FOO"))
            """,
            id="UseJinjaVariableGet | SKIP | cannot import module",
        ),
        pytest.param(
            UseJinjaVariableGet,
            """
            from airflow.models import Variable

            def foo():
                from operators.fake import FakeOperator

            FakeOperator(task_id="fake", foo=Variable.get("FOO"))
            """,
            """
            from airflow.models import Variable

            def foo():
                from operators.fake import FakeOperator

            FakeOperator(task_id="fake", foo=Variable.get("FOO"))
            """,
            id="UseJinjaVariableGet | SKIP | cannot reach import",
        ),
        pytest.param(
            UseJinjaVariableGet,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            FakeOperator(task_id="fake", fizz=Variable.get("FOO"))
            """,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            FakeOperator(task_id="fake", fizz=Variable.get("FOO"))
            """,
            id="UseJinjaVariableGet | SKIP | keyword not in template_fields",
        ),
        pytest.param(
            UseJinjaVariableGet,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            var = Variable.get("FOO")

            FakeOperator(task_id="fake", foo=var)
            """,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            var = '{{ var.value.FOO }}'

            FakeOperator(task_id="fake", foo=var)
            """,
            id="UseJinjaVariableGet | SUCCESS | variable assignment",
        ),
        pytest.param(
            UseJinjaVariableGet,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            def foo():
                var = Variable.get("FOO")

            FakeOperator(task_id="fake", foo=var)
            """,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            def foo():
                var = Variable.get("FOO")

            FakeOperator(task_id="fake", foo=var)
            """,
            id="UseJinjaVariableGet | SKIP | cannot reach variable",
        ),
        pytest.param(
            UseJinjaVariableGet,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            var = Variable.get("FOO")

            FakeOperator(task_id="fake", foo=var, bar=var)
            """,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            var = '{{ var.value.FOO }}'

            FakeOperator(task_id="fake", foo=var, bar=var)
            """,
            id="UseJinjaVariableGet | SUCCESS | variable assignment with multiple keywords",
        ),
        pytest.param(
            UseJinjaVariableGet,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            var = Variable.get("FOO")

            FakeOperator(task_id="fake", foo=var, fizz=var)
            """,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            var = Variable.get("FOO")

            FakeOperator(task_id="fake", foo=var, fizz=var)
            """,
            id="UseJinjaVariableGet | SKIP | variable assignment at least one keyword not in template_fields",
        ),
        pytest.param(
            UseJinjaVariableGet,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            var = Variable.get("FOO")

            FakeOperator(task_id="fake", foo=var)
            FakeOperator(task_id="fake2", fizz=var)
            """,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            var = Variable.get("FOO")

            FakeOperator(task_id="fake", foo=var)
            FakeOperator(task_id="fake2", fizz=var)
            """,
            id="UseJinjaVariableGet | SKIP | variable assignment for multiple calls where at least one keyword is not in template_fields",
        ),
        pytest.param(
            UseJinjaVariableGet,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            var = Variable.get("FOO")

            FakeOperator(task_id="fake", foo=var)
            FakeOperator(task_id="fake2", foo=var)
            """,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            var = '{{ var.value.FOO }}'

            FakeOperator(task_id="fake", foo=var)
            FakeOperator(task_id="fake2", foo=var)
            """,
            id="UseJinjaVariableGet | SUCCESS | variable assignment for multiple calls",
        ),
        pytest.param(
            UseJinjaVariableGet,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            FakeOperator(task_id="fake", fizz=str(Variable.get("FOO")))
            """,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            FakeOperator(task_id="fake", fizz=str(Variable.get("FOO")))
            """,
            id="UseJinjaVariableGet | SKIP | direct assignment with unimplemented parent type",
        ),
        pytest.param(
            UseJinjaVariableGet,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            FakeOperator(task_id="fake", foo=Variable.get("FOO", deserialize_json=True))
            """,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            FakeOperator(task_id="fake", foo='{{ var.json.FOO }}')
            """,
            id="UseJinjaVariableGet | SUCCESS | direct assignment with key and deserialize_json keyword",
        ),
        pytest.param(
            UseJinjaVariableGet,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            FakeOperator(task_id="fake", foo=Variable.get("FOO", default_var="BAR"))
            """,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            FakeOperator(task_id="fake", foo="{{ var.value.get('FOO', 'BAR') }}")
            """,
            id="UseJinjaVariableGet | SUCCESS | direct assignment with key and default_var keyword",
        ),
        pytest.param(
            UseJinjaVariableGet,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            FakeOperator(task_id="fake", foo=Variable.get("FOO", default_var=None))
            """,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            FakeOperator(task_id="fake", foo="{{ var.value.get('FOO', None) }}")
            """,
            id="UseJinjaVariableGet | SUCCESS | direct assignment with key and default_var=None keyword",
        ),
        pytest.param(
            UseJinjaVariableGet,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            FakeOperator(task_id="fake", foo=Variable.get("FOO", deserialize_json=True, default_var="BAR"))
            """,
            """
            from airflow.models import Variable
            from operators.fake import FakeOperator

            FakeOperator(task_id="fake", foo="{{ var.json.get('FOO', 'BAR') }}")
            """,
            id="UseJinjaVariableGet | SUCCESS | direct assignment with key, deserialize_json and default_var keywords",
        ),
    ],
)
def test_rules(rule, source, expected):
    source = textwrap.dedent(source)
    expected = textwrap.dedent(expected)

    session = Session(rules=[rule])
    assert session.run(source) == expected
