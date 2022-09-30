# Pull Revision Pipeline

This pull revision pipeline stage takes two command line arguments (and no file) as input and outputs a single file in [vref.txt](../../../fixtures/vref.txt) format containing the `verseText` entries for the given revision. 

## Local build and testing

To build and test pulling a Bible revision:

```
$ export REGISTRY=<the docker registry you are using>
$ make
$ export AQUA_DB=<the postgres connection string to your AQuA DB>
$ make test
```

The output should look like:

```
docker run --rm -it -e AQUA_DB=<your AQuA DB> <your Registry>/aqua-pipeline-pull-revision:latest pytest
============================= test session starts =============================
platform linux -- Python 3.8.14, pytest-7.1.2, pluggy-1.0.0
rootdir: /app
collected 12 items                                                            

test_db_connect.py ....                                                 [ 33%]
test_pull_rev.py .....                                                  [ 75%]
test_pull_rev_output.py ...                                             [100%]

============================= 12 passed in 4.93s ==============================
```

## Local execution

To run the pipeline stage locally:

```
?
```