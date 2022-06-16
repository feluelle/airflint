import ast
import importlib

from refactor import ReplacementAction, Rule
from refactor.context import Ancestry, Scope

from airflint.representatives.import_finder import ImportFinder


class UseJinjaVariableGet(Rule):
    """Replace `Variable.get("foo")` Calls through the jinja equivalent `{{ var.value.foo }}` if the variable is listed in `template_fields`."""

    context_providers = (Scope, Ancestry, ImportFinder)

    def match(self, node):
        assert (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "Variable"
            and node.func.attr == "get"
            and isinstance(node.func.ctx, ast.Load)
            and isinstance(variable := node.args[0], ast.Constant)
        )
        assert (
            (parents := self.context["ancestry"].get_parents(node))
            and isinstance(operator_keyword := next(parents), ast.keyword)
            and isinstance(operator_call := next(parents), ast.Call)
            and isinstance(operator_call.func, ast.Name)
            and (
                import_node := self.context["import_finder"].collect(
                    operator_call.func.id,
                    scope=self.context["scope"].resolve(operator_call.func),
                )
            )
            and import_node.module
        )
        try:
            _module = importlib.import_module(import_node.module)
        except ImportError:
            return
        with open(_module.__file__) as file:
            module = ast.parse(file.read())
        assert any(
            isinstance(stmt, ast.AnnAssign)
            and isinstance(stmt.target, ast.Name)
            and stmt.target.id == "template_fields"
            and isinstance(stmt.value, ast.Tuple)
            and any(
                isinstance(elt, ast.Constant) and elt.value == operator_keyword.arg
                for elt in stmt.value.elts
            )
            for module_stmt in module.body
            if isinstance(module_stmt, ast.ClassDef)
            for stmt in module_stmt.body
        )

        return ReplacementAction(
            node,
            target=ast.Constant(value=f"{{{{ var.value.{variable.value} }}}}"),
        )
