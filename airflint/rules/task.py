import ast
from copy import deepcopy

from refactor import ReplacementAction, Rule
from refactor.context import Scope

from airflint.utils import AddNewImport, ImportFinder, PythonCallableFinder


class _AddTaskDecoratorImport(Rule):
    """Add import for @task decorator."""

    context_providers = (Scope, ImportFinder)

    def match(self, node):
        assert isinstance(node, ast.ImportFrom)
        assert node.module == "airflow.operators.python"
        assert any(
            alias.name in ["PythonOperator", "PythonVirtualenvOperator"]
            for alias in node.names
        )

        current_scope = self.context["scope"].resolve(node)
        assert not self.context["import_finder"].collect("task", scope=current_scope)

        return AddNewImport(node, module="airflow.decorators", names=["task"])


class _AddTaskDecorator(Rule):
    """Add @task decorator for python functions to transform them into airflow tasks."""

    context_providers = (Scope, PythonCallableFinder)

    def match(self, node):
        assert isinstance(node, ast.FunctionDef)
        assert not any(
            decorator.func.id == "task"
            for decorator in node.decorator_list
            if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Name)
        ) and not any(
            decorator.func.value.id == "task" and decorator.func.attr == "virtualenv"
            for decorator in node.decorator_list
            if isinstance(decorator, ast.Call)
            and isinstance(decorator.func, ast.Attribute)
            and isinstance(decorator.func.value, ast.Name)
        )
        python_callable_finder = self.context["python_callable_finder"]
        python_operator = python_callable_finder.collect(
            node.name,
            scope=self.context["scope"].resolve(node),
        )
        assert python_operator
        assert isinstance(python_operator.func, ast.Name)
        TASK_MAPPING = {
            "PythonOperator": ast.Name(id="task", ctx=ast.Load()),
            "PythonVirtualenvOperator": ast.Attribute(
                value=ast.Name(id="task", ctx=ast.Load()),
                attr="virtualenv",
                ctx=ast.Load(),
            ),
        }
        decorator = TASK_MAPPING.get(python_operator.func.id)
        assert decorator

        replacement = deepcopy(node)
        replacement.decorator_list.append(
            ast.Call(
                func=decorator,
                args=[],
                keywords=[
                    keyword
                    for keyword in python_operator.keywords
                    if keyword.arg not in ["python_callable", "op_args", "op_kwargs"]
                ],
            ),
        )
        return ReplacementAction(node, replacement)


class _ReplacePythonOperatorByFunctionCall(Rule):
    """Replace PythonOperator calls by function calls which got decorated with the @task decorator."""

    def match(self, node):
        assert isinstance(node, (ast.Expr, ast.Assign))
        assert isinstance(node.value, ast.Call)
        assert isinstance(node.value.func, ast.Name)
        assert node.value.func.id in ["PythonOperator", "PythonVirtualenvOperator"]
        assert isinstance(node.value.func.ctx, ast.Load)

        replacement = deepcopy(node)

        args = next(
            (
                keyword.value.elts
                for keyword in node.value.keywords
                if keyword.arg == "op_args"
            ),
            None,
        )
        kwargs = next(
            (
                keyword.value.keywords
                for keyword in node.value.keywords
                if keyword.arg == "op_kwargs"
            ),
            None,
        )
        replacement.value = ast.Call(
            func=next(
                keyword.value
                for keyword in node.value.keywords
                if keyword.arg == "python_callable"
            ),
            args=[args] if args else [],
            keywords=[kwargs] if kwargs else [],
        )
        return ReplacementAction(node, replacement)


EnforceTaskFlowApi = [
    _AddTaskDecoratorImport,
    _AddTaskDecorator,
    _ReplacePythonOperatorByFunctionCall,
]
