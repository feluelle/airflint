from refactor import run

from airflint.rules import dag, task, variable


def main():
    rules = []
    rules.extend(dag.EnforceStaticStartDate)
    rules.extend(dag.EnforceTaskFlowApi)
    rules.extend(task.EnforceTaskFlowApi)
    rules.append(variable.ReplaceVariableGetByJinja)
    run(rules=rules)


if __name__ == "__main__":
    main()
