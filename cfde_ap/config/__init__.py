import os

from mdf_toolbox import dict_merge

from .acls import DEFAULT_ACLS
from .base import BASE_CONFIG
from .catalogs import KNOWN_CATALOGS
from .dev import DEV
from .keys import KEYS
from .prod import PROD
from .schemas import INPUT_SCHEMA, OUTPUT_SCHEMA
from .staging import STAGING

# Config setup
CONFIG = {
    "INPUT_SCHEMA": INPUT_SCHEMA,
    "OUTPUT_SCHEMA": OUTPUT_SCHEMA,
    "DEFAULT_ACLS": DEFAULT_ACLS,
    "KNOWN_CATALOGS": KNOWN_CATALOGS
}
CONFIG = dict_merge(BASE_CONFIG, CONFIG)
CONFIG = dict_merge(KEYS, CONFIG)

# Server-specific config will overwrite previous base values if any
server = os.environ.get("FLASK_ENV")
if server == "prod":
    CONFIG = dict_merge(PROD, CONFIG)
if server == "staging":
    CONFIG = dict_merge(STAGING, CONFIG)
elif server == "dev":
    CONFIG = dict_merge(DEV, CONFIG)
else:
    raise EnvironmentError("FLASK_ENV not correctly set! FLASK_ENV must be 'prod', 'staging',"
                           " or 'dev' to use any part of this Action Provider.")

# These should fail on portal startup if they don't exist
if not os.path.exists(CONFIG["DATA_DIR"]):
    raise EnvironmentError(f"DATA_DIR '{CONFIG['DATA_DIR']}' does not exist.")
if not os.path.exists(CONFIG["DERIVA_SCHEMA_LOCATION"]):
    raise EnvironmentError(f"DERIVA_SCHEMA_LOCATION '{CONFIG['DERIVA_SCHEMA_LOCATION']}' "
                           f"does not exist.")
