## Intent

This is more about comments than code, compiling this program on its own is fairly useless.
But, it is doable. Read below for guidelines if you want to execute!

## Requirements

Python 3, with requirements installed:

```bash
pip install -r requirements.txt
```

And clone down the repo :

```bash
git clone this-repo@github.com
```

## Execution

I separated the logic (`solution.py`) and the application (`test.py`),
in order that the logic be testable against other datasets. The tests import the solution and run it across the provided data.

You can execute the tests like so:

```bash

pytest test.py # use -s flag to print logs to stdout

# or to run a specific test:

pytest test.py::test_problem_1

pytest test.py::test_problem_2
```
