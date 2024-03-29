# Credits go to GitHub user @isidentical who provided most of the solution.
import ast

from refactor import Rule, context
from refactor.actions import BaseAction
from refactor.context import ScopeType

from airflint.actions.new_statements import NewStatementsAction


class UseFunctionLevelImports(Rule):
    """Replace top-level import by function-level import."""

    context_providers = (context.Scope,)

    def match(self, node: ast.AST) -> BaseAction:
        # Instead of going import -> function, we are going
        # function -> import since this way it is easier to
        # do the refactorings one by one and finally remove
        # all unused imports at the end.
        assert isinstance(node, ast.FunctionDef)
        # Skip functions using dag decorator
        # as they are being called by the DAG Parser as well.
        assert not any(
            isinstance(identifier, ast.Name)
            and isinstance(identifier.ctx, ast.Load)
            and identifier.id == "dag"
            for expr in node.decorator_list
            for identifier in ast.walk(expr)
        )

        inlined_imports = []
        # We only walk through function body - not decorators
        # as they are not within the function scope.
        for stmt in node.body:
            for identifier in ast.walk(stmt):
                # Find all used identifiers inside this function.
                if not (
                    isinstance(identifier, ast.Name)
                    and isinstance(identifier.ctx, ast.Load)
                ):
                    continue

                # And try to resolve their scope. Each scope has its own parent
                # (unless it is a module), so we are simply going back until we
                # find the definition for the selected identifier.
                scope = self.context["scope"].resolve(identifier)
                while not (definitions := scope.definitions.get(identifier.id, [])):
                    scope = scope.parent
                    if scope is None:
                        # There might be some magic, so let's not
                        # forget the chance of running out of scopes.
                        break

                # If any of the definitions (there might be multiple of them)
                # we found matches an import, we'll filter them out.
                imports = [
                    definition
                    for definition in definitions
                    if isinstance(definition, (ast.Import, ast.ImportFrom))
                ]
                # And we'll ensure this import is originating from the global scope.
                if imports and scope.scope_type is ScopeType.GLOBAL:
                    inlined_imports.append(imports[0])

        # We only want unique imports i.e. no duplicates
        unique_inlined_imports = list(dict.fromkeys(inlined_imports))

        # We want this rule to only run if there is at least 1 inlined import.
        assert len(unique_inlined_imports) >= 1

        # We'll select the first statement, which will act like an anchor to us
        # when we are inserting.
        first_stmt = node.body[0]
        return NewStatementsAction(first_stmt, unique_inlined_imports)
