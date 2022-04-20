import ast
from dataclasses import dataclass

from refactor import NewStatementAction


@dataclass
class AddNewImport(NewStatementAction):
    """
    Add a new From-Import statement.

    :param module: The import module.
    :param names: The list of names used in alias'.
    """

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
    """
    Add a new Call.

    :param name: The name of the Call to add.
    """

    name: str

    def build(self) -> ast.AST:
        return ast.Call(
            ast.Name(self.name, ast.Load()),
            args=[],
            keywords=[],
        )
