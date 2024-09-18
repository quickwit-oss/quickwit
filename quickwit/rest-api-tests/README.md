# Rest API tests

This directory is meant to test quickwit at the Rest API level.
It was initially meant to iterate over the elastic search compatibility API,
but it can also be used as a convenient way to create integration tests.

# Setting up the Python environment

## Installing Pipenv

```bash
pip install --user pipenv
```

[Pipenv installation](https://pipenv.pypa.io/en/latest/installation/)

## Installing the dependencies in a virtual environment

```bash
pipenv shell
pipenv install
```

# Running the tests

The test script is meant to target `elasticsearch` and `quickwit`.

When targeting quickwit, the script expects a fresh quickwit instance
running on `http://localhost:7280`. The data involved is small, and
running in DEBUG mode is fine.

```bash
./run_tests.py --engine quickwit
```

When targeting elasticsearch, the script expects elastic to be running on
`http://localhost:9200` (see [compose script](./docker-compose.yaml)).

In both cases, the test will take care of setting up, ingesting and tearing down the
indexes involved.

```./run_tests.py --engine elasticsearch```

# Writing a new test suite

Writing a new test suite only requires to create a new subdirectory somewhere in the scenarii/` tree.
The test script recursively browse the directories and executes some setup / teardown operation.

## setup

Setup consists in two things. First a context is built by loading and merging the content of the files `_ctx.yaml` and `_ctx.<engine>.yaml`.
This context will be used to prepopulate our steps dictionary.

This engine-specific context is perfect if you know all steps will target a specific endpoint, or a specific method.

Once the context is loaded, the steps described in `_setup.yaml` and `_setup.<engine>.yaml` (if present) will be executed.

These steps are just like any other steps except you are guaranteed they will be executed respectively before and after all other steps.
In particular, when targeting one specific test using the `--test flag`,
the necessary `setup` and `teardown` script will be automatically executed.

# teardown

It then executes the tests described in .yaml files, in their lexicographical order.
A single file can contain more than one tests, by separating them by `---`.

Here is an example of a test

```yaml
# Query string takes priority over query defined in body
method: [GET, POST]
params:
  # this overrides the query sent in body
  q: type:PushEvent
  size: 3
json:
  query:
    term:
      type:
        value: "whatever"
expected:
  hits:
    total:
      value: 60
      relation: "eq"
    hits:
      $expect: "len(val) == 3"
```

A test will just run a REST HTTP call, and check that the resulting JSON matches
some expectation.


- **method**: gives the list of HTTP methods to test. If there is more than one, they will be all tested.
- **params**: describes the parameters that should be sent as query strings.
- **json**: describes the JSON body, sent with the query
- **expected**: describes the expectation.

# Expectations

The expectation is an object that mirrors the structure of the response.
It does not need to contain its entire tree.

For instance, given the following json object:
```json
{"name": "Droopy", "age": 31}
```

It is possible to test for the name part only by using the following expectation:
```yaml
# ...
expected:
  name: Droopy
```

Sometimes, it might be cumbersome or even impossible to check a result against a value.
In that case, it is possible to express the condition as a python expression, by using the reserved keyword "$expect".

In the following, we could check that the age is greater than 30, like this:
```yaml
# ...
expected:
  age:
      $expect: "val >= 3"
```

Note that the value of the node (here `31`) is injected as a variable `val` in the expression.
