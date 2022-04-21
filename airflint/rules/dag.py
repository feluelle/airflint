import ast

import pendulum
from refactor import ReplacementAction, Rule
from refactor.context import Ancestry, Scope

from airflint.actions import AddNewCall, AddNewImport
from airflint.representatives import DecoratorFinder, ImportFinder
from airflint.utils import toidentifier


class _AddDagDecoratorImport(Rule):
    context_providers = (Scope, DecoratorFinder, ImportFinder)

    def match(self, node: ast.AST) -> AddNewImport:
        assert (
            isinstance(node, ast.ImportFrom)
            and node.module == "airflow"
            and any(alias.name == "DAG" for alias in node.names)
        )
        node_scope = self.context["scope"].resolve(node)
        assert self.context["decorator_finder"].collect("dag", scope=node_scope)
        assert not self.context["import_finder"].collect("dag", scope=node_scope)

        return AddNewImport(node, module="airflow.decorators", names=["dag"])


class _ReplaceDagContextManagerByDagDecorator(Rule):
    def match(self, node):
        assert isinstance(node, ast.With)
        assert (
            DAG := next(
                (
                    item.context_expr
                    for item in node.items
                    if isinstance(item.context_expr, ast.Call)
                    and isinstance(item.context_expr.func, ast.Name)
                    and item.context_expr.func.id == "DAG"
                    and isinstance(item.context_expr.func.ctx, ast.Load)
                ),
                None,
            )
        )
        assert (
            dag_id := next(
                (
                    keyword.value.value
                    for keyword in DAG.keywords
                    if keyword.arg == "dag_id"
                    and isinstance(keyword.value, ast.Constant)
                ),
                "",
            )
        )

        return ReplacementAction(
            node,
            target=ast.FunctionDef(
                name=toidentifier(dag_id),
                args=ast.arguments(
                    posonlyargs=[],
                    args=[],
                    kwonlyargs=[],
                    kw_defaults=[],
                    defaults=[],
                ),
                body=node.body,
                decorator_list=[
                    ast.Call(
                        func=ast.Name(id="dag", ctx=ast.Load()),
                        args=[],
                        keywords=[
                            keyword for keyword in DAG.keywords if keyword.arg != "dag"
                        ],
                    ),
                ],
                lineno=node.lineno,
            ),
        )


class _AddDagCall(Rule):
    def match(self, node: ast.AST) -> AddNewCall:
        assert isinstance(node, ast.FunctionDef)
        assert any(
            isinstance(decorator, ast.Call)
            and isinstance(decorator.func, ast.Name)
            and decorator.func.id == "dag"
            for decorator in node.decorator_list
        )
        assert not (
            isinstance(last_stmt := self.context.tree.body[-1], ast.Expr)
            and isinstance(call := last_stmt.value, ast.Call)
            and isinstance(call.func, ast.Name)
            and call.func.id == node.name
        )

        return AddNewCall(last_stmt, node.name)


EnforceTaskFlowApi = [
    _ReplaceDagContextManagerByDagDecorator,
    _AddDagDecoratorImport,
    _AddDagCall,
]


class _AddDatetimeImport(Rule):
    context_providers = (Scope, ImportFinder)

    def match(self, node: ast.AST) -> AddNewImport:
        assert (
            isinstance(node, ast.ImportFrom)
            and node.module == "airflow.utils.dates"
            and any(alias.name == "days_ago" for alias in node.names)
        )
        assert not self.context["import_finder"].collect(
            "datetime",
            scope=self.context["scope"].resolve(node),
        )

        return AddNewImport(node, module="pendulum", names=["datetime"])


class _ReplaceStartDateDaysAgoByDatetime(Rule):
    context_providers = (Ancestry,)

    def match(self, node: ast.AST) -> ReplacementAction:
        assert (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and isinstance(node.func.ctx, ast.Load)
            and node.func.id == "days_ago"
            and isinstance(days_ago := node.args[0], ast.Constant)
        )
        parents = self.context["ancestry"].get_parents(node)
        assert (
            isinstance(dag_keyword := next(parents), ast.keyword)
            and dag_keyword.arg == "start_date"
        )
        assert (
            isinstance(dag := next(parents), ast.Call)
            and isinstance(dag.func, ast.Name)
            and isinstance(dag.func.ctx, ast.Load)
            and dag.func.id in ["DAG", "dag"]
        )
        assert any(
            keyword.arg == "schedule_interval"
            and isinstance(keyword.value, ast.Constant)
            and keyword.value.value
            for keyword in dag.keywords
        )

        datetime = pendulum.today(tz=pendulum.UTC).subtract(days=days_ago.value)
        return ReplacementAction(
            node,
            target=ast.Call(
                func=ast.Name(id="datetime", ctx=ast.Load()),
                args=[
                    ast.Constant(value=datetime.year),
                    ast.Constant(value=datetime.month),
                    ast.Constant(value=datetime.day),
                ],
                keywords=node.keywords,
            ),
        )


EnforceStaticStartDate = [
    _AddDatetimeImport,
    _ReplaceStartDateDaysAgoByDatetime,
]
