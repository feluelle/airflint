# Credits go to GitHub user @isidentical who provided most of the solution.
import ast

from refactor import Action, Rule, context
from refactor.context import ScopeType

from airflint.actions.new_statements import NewStatementsAction


class UseFunctionLevelImports(Rule):
    """Replace top-level import by function-level import."""

    context_providers = (context.Scope,)

    def match(self, node: ast.AST) -> Action:
        # Instead of going import -> function, we are going
        # function -> import since this way it is easier to
        # do the refactorings one by one and finally remove
        # all unused imports at the end.
        assert isinstance(node, ast.FunctionDef)

        inlined_imports = []
        for identifier in ast.walk(node):
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

        # We want this rule to only run if there is at least 1 inlined import.
        assert len(inlined_imports) >= 1

        # We'll select the first statement, which will act like an anchor to us
        # when we are inserting.
        first_stmt = node.body[0]
        return NewStatementsAction(first_stmt, inlined_imports)
