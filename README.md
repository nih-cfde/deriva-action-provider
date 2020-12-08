# deriva-action-provider
Globus Automate action provider for DERIVA

## Configuration

### Setup Env

The AP dev/staging/prod servers are configured using the FLASK_ENV variable.
Set this before running the server:

    export FLASK_ENV=dev


The following needs to be set in `cfde_ap/config/keys.py`:

```
KEYS = {
    "GLOBUS_SECRET": "",
    "AWS_KEY": "",
    "AWS_SECRET": "",
    "TEMP_REFRESH_TOKEN": "",
}
```

* GLOBUS_SECRET -- The Confidential Client secret generated on developers.globus.org
* AWS_KEY/SECRET -- CFDE AWS Credentials generated on AWS
    * NOTE: This MUST have permissions to read/write on DynamoDB Tables.
* TEMP_REFRESH_TOKEN -- A Client Deriva Refresh token that's in the proper group so it can ingest

### DynamoDB

Each deployment on dev/staging/prod has its own AWS DynamoDB Table for storing
the state for active running flows. The current deployment is tracked using the
FLASK_ENV env var. First set the environment, then run the code snippet below.
The available environments are: "prod", "staging", or "dev"

Create a table on staging using the following: 

```
# Run "export FLASK_ENV=staging" first!
from cfde_ap import utils
dynamodb.Table(name="staging-ap-actions")
```

### cfde-deriva

The versions of metadata which can be ingested into Deriva are controlled by
the cfde-deriva repo version. The current version on master is `epic1_202009`
which is the only valid version which can currently be used. At some point, it
will be replaced by the epic2 version at some point. 

Epic1 verified: f9e3c9b6a446c73f0090aff7770a376a0d4e4f7c

## Deployment

### Code Deployments

1. Push code to the relevant server.
1. Restart the server with `stop_ap.sh` and `start_ap.sh`
