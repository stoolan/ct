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

## Organization

I separated the logic (`solution.py`) and the application (`test.py`),
in order that the logic be testable against other datasets. The tests import the solution and run it across the provided data.

I realized my solutions got a little buried in the tests. I designed this really for maximum testability, because to me, the code that proves the solution is correct, is as important as the solution. Unfortunately I sacrificed a little readability, but if you're familiar with python, pandas, and testing frameworks,
you should be able to follow along. That said I'll note the line nums of the solutions here for quick access:

The core logic for implementing problem 1 is in `test.py` within the function `test_problem_1`, circa line 66.

The core logic for implementing problem 1 is in `test.py` within the function `test_problem_2`, circa line 166.


# Execution

You can execute the tests like so:

```bash

pytest test.py # use -s flag to print logs to stdout

# or to run a specific test:

pytest test.py::test_problem_1

pytest test.py::test_problem_2
```
