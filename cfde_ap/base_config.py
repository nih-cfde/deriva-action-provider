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
    "DATA_DIR": os.path.join(os.path.expanduser("~"), "deriva_data"),
    "DEFAULT_SERVER_NAME": "demo.derivacloud.org",
    "DERIVA_SCHEMA_NAME": "CFDE",
    "DERIVA_SCHEMA_LOCATION": ("https://raw.githubusercontent.com/nih-cfde/cfde-deriva/"
                               "master/table-schema/demo-202006-model.json"),
    "FAIR_RE_URL": "https://317ec.36fe.dn.glob.us",
    "TRANSFER_PING_INTERVAL": 60,  # Seconds
    "TRANSFER_DEADLINE": 24 * 60 * 60  # 1 day, in seconds
}
