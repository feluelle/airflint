import ast
from typing import Optional

from refactor import Representative
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
