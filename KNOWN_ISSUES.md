# Known Issues

The following issues are not resolved yet and therefore require manual intervention to solve them:

- [F841](https://www.flake8rules.com/rules/F841.html) and [F821](https://www.flake8rules.com/rules/F821.html) can appear when the DAG context manager was replaced by the dag decorator and uses are outside of the function scope
