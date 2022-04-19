import ast

import pendulum
from refactor import ReplacementAction, Rule
from refactor.context import Ancestry, Scope

from airflint.utils import (
    AddNewCall,
    AddNewImport,
    ImportFinder,
    get_expr,
    toidentifier,
)


class _AddDagDecoratorImport(Rule):
    context_providers = (Scope, ImportFinder)

    def match(self, node):
        assert isinstance(node, ast.ImportFrom)
        assert node.module == "airflow"
        assert any(alias.name == "DAG" for alias in node.names)

        current_scope = self.context["scope"].resolve(node)
        assert not self.context["import_finder"].collect("dag", scope=current_scope)

        return AddNewImport(node, module="airflow.decorators", names=["dag"])


class _ReplaceDagContextManagerByDagDecorator(Rule):
    def get_DAG(self, withitem: ast.withitem) -> ast.Call:
        assert isinstance(withitem.context_expr, ast.Call)
        assert isinstance(withitem.context_expr.func, ast.Name)
        assert withitem.context_expr.func.id == "DAG"
        assert isinstance(withitem.context_expr.func.ctx, ast.Load)
        return withitem.context_expr

    def match(self, node):
        assert isinstance(node, ast.With)

        DAG = next(map(self.get_DAG, node.items))

        replacement = ast.FunctionDef(
            name=toidentifier(
                next(
                    keyword.value.value
                    for keyword in DAG.keywords
                    if keyword.arg == "dag_id"
                    and isinstance(keyword.value, ast.Constant)
                ),
            ),
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
                    keywords=DAG.keywords,
                ),
            ],
            lineno=node.lineno,
        )
        return ReplacementAction(node, replacement)


class _AddDagCall(Rule):
    def match(self, node):
        assert isinstance(node, ast.FunctionDef)
        assert any(
            decorator.func.id == "dag"
            for decorator in node.decorator_list
            if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Name)
        )

        last_stmt = self.context.tree.body[-1]
        if (
            isinstance(last_stmt, ast.Expr)
            and isinstance(call := last_stmt.value, ast.Call)
            and isinstance(call.func, ast.Name)
            and call.func.id == node.name
        ):
            return None

        return AddNewCall(last_stmt, node.name)


EnforceTaskFlowApi = [
    _AddDagDecoratorImport,
    _ReplaceDagContextManagerByDagDecorator,
    _AddDagCall,
]


class _AddDatetimeImport(Rule):
    context_providers = (Scope, ImportFinder)

    def match(self, node):
        assert isinstance(node, ast.ImportFrom)
        assert node.module == "airflow.utils.dates"
        assert any(alias.name == "days_ago" for alias in node.names)

        current_scope = self.context["scope"].resolve(node)
        assert not self.context["import_finder"].collect(
            "datetime",
            scope=current_scope,
        )

        return AddNewImport(node, module="pendulum", names=["datetime"])


class _ReplaceStartDateDaysAgoByDatetime(Rule):
    context_providers = (Ancestry,)

    def _validate_days_ago(self, node: ast.AST) -> None:
        assert isinstance(node, ast.Call)
        assert isinstance(node.func, ast.Name)
        assert isinstance(node.func.ctx, ast.Load)
        assert node.func.id == "days_ago"

    def match(self, node):
        self._validate_days_ago(node)
        assert isinstance(expr := get_expr(node, keyword_arg="n"), ast.Constant)

        parents = self.context["ancestry"].get_parents(node)

        assert isinstance(dag_keyword := next(parents), ast.keyword)
        assert dag_keyword.arg == "start_date"

        assert isinstance(dag := next(parents), ast.Call)
        assert isinstance(dag.func, ast.Name)
        assert isinstance(dag.func.ctx, ast.Load)
        assert dag.func.id in ["DAG", "dag"]

        schedule_interval = next(
            (
                keyword
                for keyword in dag.keywords
                if keyword.arg == "schedule_interval"
                and isinstance(keyword.value, ast.Constant)
                and keyword.value.value
            ),
            None,
        )
        assert schedule_interval

        datetime = pendulum.today(tz=pendulum.UTC).subtract(days=expr.value)
        replacement = ast.Call(
            func=ast.Name(id="datetime", ctx=ast.Load()),
            args=[
                ast.Constant(value=datetime.year),
                ast.Constant(value=datetime.month),
                ast.Constant(value=datetime.day),
            ],
            keywords=node.keywords,
        )
        return ReplacementAction(node, replacement)


EnforceStaticStartDate = [
    _AddDatetimeImport,
    _ReplaceStartDateDaysAgoByDatetime,
]
