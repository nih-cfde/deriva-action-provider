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
}
```

* GLOBUS_SECRET -- The Confidential Client secret generated on developers.globus.org
* AWS_KEY/SECRET -- CFDE AWS Credentials generated on AWS
    * NOTE: This MUST have permissions to read/write on DynamoDB Tables.
    
### Local Development

With the KEYS above set, you can run the server locally with:

    export FLASK_ENV=dev
    export FLASK_APP=cfde_ap/api.py
    flask run
    
The action provider can be tested with data by using the globus-automate tool
to call the /run endpoint. This simulates the flow calling the DerivaIngest
action. **Note**: The flow automatically copies data to the GCS endpoint, but
calling the action provider directly will skip this step. You must ensure your
test data is already on the GCS endpoint for your local server to pull it down.

Given the following input test data test.json:

```
{
  "data_url": "https://g-c7e94.f19a4.5898.data.globus.org/CFDE/data/KF_C2M2_submission.tgz",
  "dcc_id": "cfde_registry_dcc:kidsfirst",
  "globus_ep": "36530efa-a1e3-45dc-a6e7-9560a8e9ac49",
  "operation": "ingest",
  "test_sub": false
}
```

The following can be submitted to your locally running server with:

```
globus-automate action run \
--action-url http://localhost:5000 \
--action-scope https://auth.globus.org/scopes/21017803-059f-4a9b-b64c-051ab7c1d05d/demo \
--body test.json
```

## Deployment

There are two distinct steps to deploying the Deriva Action Provider.

1. Upgrading code that runs on the Deriva Action Provider Server
1. Upgrading the Globus Automate flow

The Globus Automate flow will call "DerivaIngest" as one of the actions within
the flow, but that is the extent of the coupling between both entities. Each
can be upgraded independently of the other.

### Code Deployments

Each remote server is setup with a remote git repository, pushing to the Git
repo will call a git `post-receive` hook, and checkout the pushed branch to the
latest. 

Track the remote git servers with the following:

    git remote add dev ap-dev.nih-cfde.org:deriva-action-provider
    
Commit your code, then push to the dev server:

    git push dev mybranch
    
**Dependencies are not automatically upgraded.** You will need to manually do a
pip install -r requirements.txt

**The service doesn't always restart correctly.** You will need to do this manually
with a `stop_ap.sh` and `start_ap.sh`

### Flow Deployments

See the deployment doc in globus_automate/deployment.md. 
