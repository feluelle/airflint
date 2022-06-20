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
            import functools
            import operator
            import dataclass

            def something():
                a = functools.reduce(x, y)
                b = operator.add(a, a)
                return b

            def other_thing():
                return dataclass(something(1, 2))
            """,
            """
            import functools
            import operator
            import dataclass

            def something():
                import functools
                import operator
                a = functools.reduce(x, y)
                b = operator.add(a, a)
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
    ],
)
def test_rules(rule, source, expected):
    source = textwrap.dedent(source)
    expected = textwrap.dedent(expected)

    session = Session(rules=[rule])
    assert session.run(source) == expected
