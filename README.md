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

## 🧑‍🏫 Rules

- [x] use datetime instead of days_ago in DAG start_date
- [x] task decorator instead of PythonOperator and PythonVenvOperator
- [x] dag decorator instead of DAG
- [x] jinja string instead of Variable.get

> ⚠️ airflint does not remove imports. For removing unused imports please use [autoflake](https://github.com/PyCQA/autoflake) additionally.

## 💡 Future Ideas

- fix Official Airflow `DeprecationWarning`s

## 🚀 Get started

To install it from [PyPI](https://pypi.org/) run:

```console
pip install airflow-diagrams
```
