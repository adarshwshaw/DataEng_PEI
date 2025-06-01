# Data Engineering Profit analysis

### Databricks setup

- import the source file into databricks as notebook
  - this is for community edition where workflows are not available
- run the db_setup notebook to setup the catelogs
- to run the pipeline upload the files into dbfs
- run the workflow_job notebook to execute the pipeline
- any new file added into the dbfs location will be loaded as incremental load


###  local Setup for testing

- Setup virtual environment for python(tested on 3.11.9)
- Setup spark version 3.5.5
- install the dependencies
```shell
pip install -r requirements.txt
```
- run the test in the test folder using pytest
