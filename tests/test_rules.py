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
            UseJinjaVariableGet,
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
            # Test that nothing happens if it cannot import the module.
            UseJinjaVariableGet,
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
            # Test that nothing happens if it is not in template_fields.
            UseJinjaVariableGet,
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            var = Variable.get("FOO")

            BashOperator(task_id="foo", output_encoding=var)
            """,
            """
            from airflow.models import Variable
            from airflow.operators.bash import BashOperator

            var = Variable.get("FOO")

            BashOperator(task_id="foo", output_encoding=var)
            """,
        ),
        (
            UseJinjaVariableGet,
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
            # Test that nothing happens if at least one keyword is not in template_fields.
            UseJinjaVariableGet,
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
            # Test that nothing happens if variable is being referenced in multiple calls where at least one keyword is not in template_fields.
            UseJinjaVariableGet,
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
