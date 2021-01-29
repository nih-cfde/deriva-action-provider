# Deployment

This directory contains extra deployment config files critical for deploying
the flow and updating the client config

### Flow Deployment

Flow deployment is a two step process:

1. Deploy the latest flow definition in Globus Automate
1. Deploy the `cfde_client_config.json` to switch cfde-submit users to the latest version

Deployment is mostly automated with the deploy.py script. A full deployment
looks like the following:

    python deploy.py flow --service dev
    python deploy.py client-config
    
Suggested you deploy the flow and test it, before deploying the `client-config`.

### Client Config

The ``cfde_client_config.json`` file lists the global configuration each user
will pull from when running the cfde-submit tool. The config exists on a
public Globus endpoint and is used by each client regardless of version.

Current Deployment exists on the GCS CFDE Prod endpoint

https://g-5cf005.aa98d.08cc.data.globus.org/submission_dynamic_config/cfde_client_config.json

#### Client Config Testing

The [cfde-submit package here](https://github.com/nih-cfde/cfde-submit) is the public
client-facing package for submitting datasets to Deriva. 

```
# pip install cfde-submit
cfde-submit run --service-instance dev datapackage_0631cefb6a1cfff0ef4ecbf6931421add27f5884.zip
```
