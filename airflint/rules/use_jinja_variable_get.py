import ast
from importlib import import_module
from typing import Any

from refactor import ReplacementAction, Rule
from refactor.context import Ancestry, Scope


class UseJinjaVariableGet(Rule):
    """Replace `Variable.get("foo")` Calls through the jinja equivalent `{{ var.value.foo }}` if the variable is listed in `template_fields`."""

    context_providers = (Scope, Ancestry)

    def _get_operator_keywords(self, reference: ast.Call) -> list[ast.keyword]:
        parent = self.context["ancestry"].get_parent(reference)

        if isinstance(parent, ast.Assign):
            # Get all operator keywords referencing the variable, Variable.get call was assigned to.
            scope = self.context["scope"]
            operator_keywords = [
                node
                for node in ast.walk(self.context.tree)
                if isinstance(node, ast.keyword)
                and isinstance(node.value, ast.Name)
                and any(
                    node.value.id == target.id
                    and scope.resolve(node.value).can_reach(scope.resolve(target))
                    for target in parent.targets
                    if isinstance(target, ast.Name)
                )
            ]
            if operator_keywords:
                return operator_keywords
            raise AssertionError("No operator keywords found. Skipping..")

        if isinstance(parent, ast.keyword):
            # Direct reference without variable assignment.
            return [parent]

        raise AssertionError(f"Unsupported parent type {type(parent)}. Skipping..")

    def _lookup_template_fields(self, keyword: ast.keyword) -> None:
        parent = self.context["ancestry"].get_parent(keyword)

        # Find the import node module matching the operator calls name.
        assert isinstance(operator_call := parent, ast.Call)
        assert isinstance(operator_call.func, ast.Name)
        scope = self.context["scope"].resolve(operator_call.func)
        try:
            import_node = next(
                node
                for node in ast.walk(self.context.tree)
                if isinstance(node, ast.ImportFrom)
                and any(alias.name == operator_call.func.id for alias in node.names)
                and scope.can_reach(self.context["scope"].resolve(node))
            )
        except StopIteration:
            raise AssertionError("Could not find import definition. Skipping..")
        assert (module_name := import_node.module)

        # Try to import the module into python.
        try:
            _module = import_module(module_name)
        except ImportError:
            raise AssertionError("Could not import module. Skipping..")
        assert (file_path := _module.__file__)

        # Parse the ast to check if the keyword is in template_fields.
        with open(file_path) as file:
            module = ast.parse(file.read())
        assert any(
            isinstance(stmt, ast.AnnAssign)
            and isinstance(stmt.target, ast.Name)
            and stmt.target.id == "template_fields"
            and isinstance(stmt.value, ast.Tuple)
            and any(
                isinstance(elt, ast.Constant) and elt.value == keyword.arg
                for elt in stmt.value.elts
            )
            for module_stmt in module.body
            if isinstance(module_stmt, ast.ClassDef)
            for stmt in module_stmt.body
        )

    def _get_parameter(
        self,
        node: ast.Call,
        position: int,
        name: str,
    ) -> Any:
        if position < len(node.args) and isinstance(
            arg := node.args[position],
            ast.Constant,
        ):
            return arg.value
        return next(
            keyword.value.value
            for keyword in node.keywords
            if keyword.arg == name and isinstance(keyword.value, ast.Constant)
        )

    def _construct_value(self, node: ast.Call) -> str:
        # Read key from Variable.get node.
        key = self._get_parameter(node, position=0, name="key")

        # Read optional deserialize_json from Variable.get node.
        try:
            deserialize_json = self._get_parameter(
                node,
                position=2,
                name="deserialize_json",
            )
            var_type = "json" if deserialize_json else "value"
        except StopIteration:
            var_type = "value"

        # Read optional default_var from Variable.get node and construct the final value.
        try:
            default_var = self._get_parameter(node, position=1, name="default_var")
            if isinstance(default_var, str):
                value = f"{{{{ var.{var_type}.get('{key}', '{default_var}') }}}}"
            else:
                value = f"{{{{ var.{var_type}.get('{key}', {default_var}) }}}}"
        except StopIteration:
            value = f"{{{{ var.{var_type}.{key} }}}}"

        return value

    def match(self, node):
        assert (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "Variable"
            and node.func.attr == "get"
            and isinstance(node.func.ctx, ast.Load)
        )

        for operator_keyword in self._get_operator_keywords(reference=node):
            self._lookup_template_fields(keyword=operator_keyword)

        return ReplacementAction(
            node,
            target=ast.Constant(value=self._construct_value(node)),
        )
