import ast
import importlib

from refactor import ReplacementAction, Rule
from refactor.context import Ancestry, Scope

from airflint.rules.dag import get_expr
from airflint.utils import ImportFinder


class ReplaceVariableGetByJinja(Rule):
    """Replace `Variable.get("foo")` Calls through the jinja equivalent `{{ var.value.foo }}` if the variable is listed in `template_fields`."""

    context_providers = (Scope, Ancestry, ImportFinder)

    def _validate_variable_get(self, node: ast.AST) -> None:
        assert isinstance(node, ast.Call)
        assert isinstance(node.func, ast.Attribute)
        assert isinstance(node.func.value, ast.Name)
        assert node.func.value.id == "Variable"
        assert node.func.attr == "get"
        assert isinstance(node.func.ctx, ast.Load)

    def _validate_templatable(self, node: ast.ImportFrom, field: str) -> None:
        assert node.module
        file_path = importlib.import_module(node.module).__file__
        assert file_path
        with open(file_path) as file:
            module = ast.parse(file.read())
        assert any(
            isinstance(stmt, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "template_fields"
                for target in stmt.targets
            )
            and isinstance(stmt.value, ast.Tuple)
            and any(
                isinstance(elt, ast.Constant) and elt.value == field
                for elt in stmt.value.elts
            )
            for module_stmt in module.body
            if isinstance(module_stmt, ast.ClassDef)
            for stmt in module_stmt.body
        )

    def match(self, node):
        self._validate_variable_get(node)
        assert isinstance(expr := get_expr(node, keyword_arg="key"), ast.Constant)

        parents = self.context["ancestry"].get_parents(node)
        # No need to check for other than ast.keyword as Airflow forces the user to use keywords in Operator calls.
        assert isinstance(operator_keyword := next(parents), ast.keyword)
        assert isinstance(operator_call := next(parents), ast.Call)
        assert isinstance(operator_call.func, ast.Name)

        import_node = self.context["import_finder"].collect(
            operator_call.func.id,
            scope=self.context["scope"].resolve(operator_call.func),
        )
        assert import_node
        self._validate_templatable(
            node=import_node,
            field=operator_keyword.arg,
        )

        replacement = ast.Constant(value=f"{{{{ var.value.{expr.value} }}}}")
        return ReplacementAction(node, replacement)
