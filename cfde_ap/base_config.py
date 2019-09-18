import os


BASE_CONFIG = {
    "LOG_LEVEL": "DEBUG",
    "API_LOG_FILE": "api.log",
    "DEMO_DYNAMO_TABLE": "cfde-demo1-actions",
    "GLOBUS_NATIVE_APP": "417301b1-5101-456a-8a27-423e71a2ae26",
    "GLOBUS_CC_APP": "21017803-059f-4a9b-b64c-051ab7c1d05d",
    "GLOBUS_SCOPE": "https://auth.globus.org/scopes/21017803-059f-4a9b-b64c-051ab7c1d05d/demo",
    "GLOBUS_AUD": "cfde_ap_demo",
    "GLOBUS_GROUP": "a437abe3-c9a4-11e9-b441-0efb3ba9a670",
    "DATA_DIR": os.path.join(os.path.dirname(__file__), "data"),
    "DERIVA_SERVER_NAME": "demo.derivacloud.org",
    "DERIVA_SCHEMA_NAME": "CFDE"
}
