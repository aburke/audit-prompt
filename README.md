# audit-prompt

## Setup
 Run the following comamnds under the project path to get started:  

```
$ pip3 install pipenv
$ export PIPENV_IGNORE_VIRTUALENVS=1
$ pipenv install 
$ pipenv shell
$ python3 replay.py -h
```

This code is run with python 3.7.  
To exit the python virtual enviroment type `deactivate` in the command line.

## Examples
Once the setup is complete you can use the replay.py script to test the code.    
Note the following assumptions for the below examples:  

- The local ".jsonl.gz" files are stored at the location **/tmp/ehub_data**
- The s3 ".jsonl.gz" files are stored at **s3://demo-bucket/audit-test**  
- The s3 credentials have already be configured  

### Local Example (based on challenge sample)
```
$ python3 replay.py --field ambientTemp --field schedule /tmp/ehub_data 2016-01-01T03:00
{'state': {'ambientTemp': 77.0, 'schedule': False}, 'ts': '2016-01-01T03:00'}
```  

### S3 Example (based on challenge sample)
```
$ python3 replay.py --field ambientTemp --field schedule s3://demo-bucket/audit-test 2016-01-01T03:00
{'state': {'ambientTemp': 77.0, 'schedule': False}, 'ts': '2016-01-01T03:00'}
```

### Local Example where one of the fields entered does not exist
```
$ python3 replay.py --field ambientTemp --field lastAlertTs --field barn /tmp/ehub_data 2016-07-05T04:10
{audit.py:210} - WARNING - The field "barn" could not be found.
{'state': {'ambientTemp': 87.0, 'lastAlertTs': '2016-07-05T01:16:00.001282'}, 'ts': '2016-07-05T04:10'}
```

### S3 Example where field shows up in a file that comes after the process date file 
"mode" field does not exist in audit-test/2016/01/09.jsonl.gz but instead is found in audit-test/2016/01/16.jsonl.gz
```
$ python3 replay.py --field mode  s3://demo-bucket/audit-test 2016-01-09T00:20
{'state': {'mode': 'AUTO'}, 'ts': '2016-01-09T00:20'}
```  

## Unit Tests
See below example for running unit tests:  
```
$ python3 test_audit.py
.......
----------------------------------------------------------------------
Ran 7 tests in 0.108s

OK
```