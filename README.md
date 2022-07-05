# airflint

[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/feluelle/airflint/main.svg)](https://results.pre-commit.ci/latest/github/feluelle/airflint/main)
![test workflow](https://github.com/feluelle/airflint/actions/workflows/test.yml/badge.svg)
![codeql-analysis workflow](https://github.com/feluelle/airflint/actions/workflows/codeql-analysis.yml/badge.svg)
[![codecov](https://codecov.io/gh/feluelle/airflint/branch/main/graph/badge.svg?token=J8UEP8IVY4)](https://codecov.io/gh/feluelle/airflint)
[![PyPI version](https://img.shields.io/pypi/v/airflint)](https://pypi.org/project/airflint/)
[![License](https://img.shields.io/pypi/l/airflint)](https://github.com/feluelle/airflint/blob/main/LICENSE)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/airflint)](https://pypi.org/project/airflint/)
[![PyPI version](https://img.shields.io/pypi/dm/airflint)](https://pypi.org/project/airflint/)

> Enforce Best Practices for all your Airflow DAGs. ⭐

⚠️ **airflint is still in alpha stage and has not been tested with real world Airflow DAGs. Please report any issues you face via [GitHub Issues](https://github.com/feluelle/airflint/issues), thank you. 🙏**

## 🧑‍🏫 Rules

- [x] Use function-level imports instead of top-level imports[^1][^2] (see [Top level Python Code](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#top-level-python-code))
- [x] Use jinja template syntax instead of `Variable.get` (see [Airflow Variables](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#airflow-variables))

[^1]: There is a PEP for [Lazy Imports](https://peps.python.org/pep-0690/) targeted to arrive in Python 3.12 which would supersede this rule.

[^2]: To remove top-level imports after running `UseFunctionLevelImports` rule, use a tool such as [autoflake](https://github.com/PyCQA/autoflake).

_based on official [Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)_

## Requirements

airflint is tested with:

|                | Main version (dev)               | Released version (0.3.2-alpha) |
|----------------|----------------------------------|--------------------------------|
| Python         | 3.9, 3.10, 3.11.0-alpha - 3.11.0 | 3.9, 3.10                      |
| Apache Airflow | >= 2.0.0                         | >= 2.0.0                       |

## 🚀 Get started

To install it from [PyPI](https://pypi.org/) run:

```console
pip install airflint
```

> **_NOTE:_** It is recommended to install airflint into your existing airflow environment with all your providers included. This way `UseJinjaVariableGet` rule can detect all `template_fields` and airflint works as expected.

Then just call it like this:

![usage](assets/images/usage.png)

### pre-commit

Alternatively you can add the following repo to your `pre-commit-config.yaml`:

```yaml
  - repo: https://github.com/feluelle/airflint
    rev: v0.3.2-alpha
    hooks:
      - id: airflint
        args: ["-a"]  # Use -a to apply the suggestions
        additional_dependencies:  # Add all package dependencies you have in your dags, preferable with version spec
          - apache-airflow
          - apache-airflow-providers-cncf-kubernetes
```

To complete the `UseFunctionlevelImports` rule, please add the `autoflake` hook after the `airflint` hook, as below:

```yaml
  - repo: https://github.com/pycqa/autoflake
    rev: v1.4
    hooks:
      - id: autoflake
        args: ["--remove-all-unused-imports", "--in-place"]
```

This will remove unused imports.

## ❤️ Contributing

I am looking for contributors who are interested in..

- testing airflint with real world Airflow DAGs and reporting issues as soon as they face them
- optimizing the ast traversing for existing rules
- adding new rules based on best practices or bottlenecks you have experienced during Airflow DAGs authoring
- documenting about what is being supported in particular by each rule
- defining supported airflow versions i.e. some rules are bound to specific Airflow features and version

For questions, please don't hesitate to open a GitHub issue.
