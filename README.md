# airflint

> A linter for your Airflow DAGs to ensure Best Practices are being used.

## 🧑‍🏫 Rules

- [x] use datetime instead of days_ago in DAG start_date
- [x] task decorator instead of PythonOperator and PythonVenvOperator
- [x] dag decorator instead of DAG
- [x] jinja string instead of Variable.get

> ⚠️ airflint does not remove imports. For removing unused imports please use [autoflake](https://github.com/PyCQA/autoflake) additionally.

## 💡 Future Ideas

- create refactorings for Airflow `DeprecationWarning`s
