import ast
import re
from dataclasses import dataclass
from typing import Optional

from refactor import NewStatementAction, Representative
from refactor.context import ScopeInfo


class ImportFinder(Representative):
    """Find import by alias name."""

    def collect(self, name: str, scope: ScopeInfo) -> Optional[ast.ImportFrom]:
        return next(
            (
                node
                for node in ast.walk(self.context.tree)
                if isinstance(node, ast.ImportFrom)
                and any(alias.name == name for alias in node.names)
                and scope.can_reach(self.context["scope"].resolve(node))
            ),
            None,
        )


class PythonCallableFinder(Representative):
    """Find python callables references in PythonOperator."""

    def collect(self, name: str, scope: ScopeInfo) -> Optional[ast.Call]:
        return next(
            (
                node
                for node in ast.walk(self.context.tree)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id in ["PythonOperator", "PythonVirtualenvOperator"]
                and any(
                    keyword.arg == "python_callable"
                    and isinstance(keyword.value, ast.Name)
                    and keyword.value.id == name
                    for keyword in node.keywords
                )
                and scope.can_reach(self.context["scope"].resolve(node))
            ),
            None,
        )


def get_expr(node: ast.Call, keyword_arg: str) -> ast.expr:
    """
    Get ast expressions for an ast Call, which have been passed either through args or kwargs.

    :param node: The node to check for expressions.
    :param keyword_arg: The keyword_arg to retrieve the values for, otherwise look only in args.
    """
    if node.args:
        return node.args[0]
    for keyword in node.keywords:
        if keyword.arg == keyword_arg:
            return keyword.value
    raise NotImplementedError


def toidentifier(string: str) -> str:
    """
    Convert to a python identifier.

    :param string: The string to convert to a valid python identifier.
    """
    return re.sub(r"\W|^(?=\d)", "_", string)


@dataclass
class AddNewImport(NewStatementAction):
    """Add a new From-Import statement."""

    module: str
    names: list[str]

    def build(self) -> ast.AST:
        return ast.ImportFrom(
            level=0,
            module=self.module,
            names=[ast.alias(name) for name in self.names],
        )


@dataclass
class AddNewCall(NewStatementAction):
    """Add a new Call."""

    name: str

    def build(self) -> ast.AST:
        return ast.Call(
            ast.Name(self.name, ast.Load()),
            args=[],
            keywords=[],
        )
