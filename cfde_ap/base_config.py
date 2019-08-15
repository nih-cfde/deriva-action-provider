import os


BASE_CONFIG = {
    "LOG_LEVEL": "DEBUG",
    "API_LOG_FILE": "api.log",
    "DEMO_DYNAMO_TABLE": "cfde-demo1-actions",
    "GLOBUS_ID": "21017803-059f-4a9b-b64c-051ab7c1d05d",
    "GLOBUS_SCOPE": "https://auth.globus.org/scopes/21017803-059f-4a9b-b64c-051ab7c1d05d/demo",
    "GLOBUS_AUD": "cfde_ap_demo",
    "DATA_DIR": os.path.join(os.path.dirname(__file__), "data")
}
