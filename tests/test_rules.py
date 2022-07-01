import textwrap

import pytest
from refactor import Session

from airflint.rules.use_function_level_imports import UseFunctionLevelImports
from airflint.rules.use_jinja_variable_get import UseJinjaVariableGet


@pytest.mark.parametrize(
    "rule, source, expected",
    [
        (
            UseFunctionLevelImports,
            # Test that all required imports within functions are being added to functions.
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
        ),
        (
            UseFunctionLevelImports,
            # Test that it skips the dag decoratored functions.
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
        ),
        (
            UseFunctionLevelImports,
            # Test that it only adds unique imports i.e. only once
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
        ),
        (
            UseFunctionLevelImports,
            # Test that it ignores imports for function decorators.
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
        ),
        (
            UseJinjaVariableGet,
            # Test that direct assignment of Variable.get is being transformed to jinja equivalent.
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
        (
            UseJinjaVariableGet,
            # Test that nothing happens if it cannot import the module.
            """
            from airflow.models import Variable
            from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

            KubernetesPodOperator(task_id="foo", bash_command=Variable.get("FOO"))
            """,
            """
            from airflow.models import Variable
            from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

            KubernetesPodOperator(task_id="foo", bash_command=Variable.get("FOO"))
            """,
        ),
        (
            UseJinjaVariableGet,
            # Test that nothing happens if the import cannot be reached.
            """
            from airflow.models import Variable

            def foo():
                from airflow.operators.bash import BashOperator

            BashOperator(task_id="foo", bash_command=Variable.get("FOO"))
            """,
            """
            from airflow.models import Variable

            def foo():
                from airflow.operators.bash import BashOperator

            BashOperator(task_id="foo", bash_command=Variable.get("FOO"))
            """,
        ),
        (
            UseJinjaVariableGet,
            # Test that nothing happens if it is not in template_fields.
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            BashOperator(task_id="foo", output_encoding=Variable.get("FOO"))
            """,
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            BashOperator(task_id="foo", output_encoding=Variable.get("FOO"))
            """,
        ),
        (
            UseJinjaVariableGet,
            # Test that variable assignment of Variable.get is being transformed to jinja equivalent.
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            var = Variable.get("FOO")

            BashOperator(task_id="foo", bash_command=var)
            """,
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            var = '{{ var.value.FOO }}'

            BashOperator(task_id="foo", bash_command=var)
            """,
        ),
        (
            UseJinjaVariableGet,
            # Test that nothing happens if the variable cannot be reached.
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            def foo():
                var = Variable.get("FOO")

            BashOperator(task_id="foo", bash_command=var)
            """,
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            def foo():
                var = Variable.get("FOO")

            BashOperator(task_id="foo", bash_command=var)
            """,
        ),
        (
            UseJinjaVariableGet,
            # Test that variable assignment works for multiple keywords.
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            var = Variable.get("FOO")

            BashOperator(task_id="foo", bash_command=var, env=var)
            """,
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            var = '{{ var.value.FOO }}'

            BashOperator(task_id="foo", bash_command=var, env=var)
            """,
        ),
        (
            UseJinjaVariableGet,
            # Test that nothing happens if at least one keyword is not in template_fields.
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            var = Variable.get("FOO")

            BashOperator(task_id="foo", bash_command=var, output_encoding=var)
            """,
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            var = Variable.get("FOO")

            BashOperator(task_id="foo", bash_command=var, output_encoding=var)
            """,
        ),
        (
            UseJinjaVariableGet,
            # Test that nothing happens if variable is being referenced in multiple Calls where at least one keyword is not in template_fields.
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            var = Variable.get("FOO")

            BashOperator(task_id="foo", bash_command=var)
            BashOperator(task_id="bar", output_encoding=var)
            """,
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            var = Variable.get("FOO")

            BashOperator(task_id="foo", bash_command=var)
            BashOperator(task_id="bar", output_encoding=var)
            """,
        ),
        (
            UseJinjaVariableGet,
            # Test that variable assignment works for multiple Calls.
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            var = Variable.get("FOO")

            BashOperator(task_id="foo", bash_command=var)
            BashOperator(task_id="bar", bash_command=var)
            """,
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            var = '{{ var.value.FOO }}'

            BashOperator(task_id="foo", bash_command=var)
            BashOperator(task_id="bar", bash_command=var)
            """,
        ),
        (
            UseJinjaVariableGet,
            # Test that nothing happens if the type of Variable.get Calls parent is not implemented e.g. function call
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            BashOperator(task_id="foo", output_encoding=str(Variable.get("FOO")))
            """,
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            BashOperator(task_id="foo", output_encoding=str(Variable.get("FOO")))
            """,
        ),
        (
            UseJinjaVariableGet,
            # Test that Variable.get calls with deserialize_json works.
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            BashOperator(task_id="foo", bash_command=Variable.get("FOO", deserialize_json=True))
            """,
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            BashOperator(task_id="foo", bash_command='{{ var.json.FOO }}')
            """,
        ),
        (
            UseJinjaVariableGet,
            # Test that Variable.get calls with default_var works.
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            BashOperator(task_id="foo", bash_command=Variable.get("FOO", default_var="BAR"))
            """,
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            BashOperator(task_id="foo", bash_command="{{ var.value.get('FOO', 'BAR') }}")
            """,
        ),
        (
            UseJinjaVariableGet,
            # Test that Variable.get calls with default_var=None works.
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            BashOperator(task_id="foo", bash_command=Variable.get("FOO", default_var=None))
            """,
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            BashOperator(task_id="foo", bash_command="{{ var.value.get('FOO', None) }}")
            """,
        ),
        (
            UseJinjaVariableGet,
            # Test that Variable.get calls works with both - deserialize_json and default_var.
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            BashOperator(task_id="foo", bash_command=Variable.get("FOO", deserialize_json=True, default_var="BAR"))
            """,
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            BashOperator(task_id="foo", bash_command="{{ var.json.get('FOO', 'BAR') }}")
            """,
        ),
    ],
)
def test_rules(rule, source, expected):
    source = textwrap.dedent(source)
    expected = textwrap.dedent(expected)

    session = Session(rules=[rule])
    assert session.run(source) == expected
