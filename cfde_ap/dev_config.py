import os
from .keys import KEYS


BASE_CONFIG = {
    "LOG_LEVEL": "DEBUG",
    "API_LOG_FILE": "api.log",
    "DEMO_DYNAMO_TABLE": "cfde-dev-actions1",
    "GLOBUS_NATIVE_APP": "417301b1-5101-456a-8a27-423e71a2ae26",
    "GLOBUS_CC_APP": "9424983e-7bd6-4cea-b589-e93e88b038d9",
    "GLOBUS_SCOPE": "",
    "GLOBUS_AUD": "",
    "GLOBUS_GROUP": "",
    "DATA_DIR": os.path.join(os.path.expanduser("~"), "deriva_data"),
    "DEFAULT_SERVER_NAME": "demo.derivacloud.org",
    "DERIVA_SCHEMA_NAME": "CFDE",
    "FAIR_RE_URL": "https://317ec.36fe.dn.glob.us",
    "TRANSFER_PING_INTERVAL": 60,  # Seconds
    "TRANSFER_DEADLINE": 24 * 60 * 60,  # 1 day, in seconds
    "GLOBUS_SECRET": KEYS["DEV_GLOBUS_SECRET"],
    "AWS_KEY": KEYS["AWS_KEY"],
    "AWS_SECRET": KEYS["AWS_SECRET"],
    "TEMP_REFRESH_TOKEN": KEYS["TEMP_REFRESH_TOKEN"]
}
