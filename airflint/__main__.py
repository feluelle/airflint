from refactor import run

from airflint.rules.use_function_level_imports import UseFunctionLevelImports
from airflint.rules.use_jinja_variable_get import UseJinjaVariableGet


def main():
    run(
        rules=[
            UseFunctionLevelImports,
            UseJinjaVariableGet,
        ],
    )


if __name__ == "__main__":
    main()
