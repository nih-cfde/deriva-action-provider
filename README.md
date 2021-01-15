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
