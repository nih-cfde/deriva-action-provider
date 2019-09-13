from mdf_toolbox import dict_merge

from .acl_config import DEFAULT_ACLS
from .base_config import BASE_CONFIG
from .keys import KEYS
from .schemas import INPUT_SCHEMA, OUTPUT_SCHEMA

# Config setup
CONFIG = {
    "INPUT_SCHEMA": INPUT_SCHEMA,
    "OUTPUT_SCHEMA": OUTPUT_SCHEMA,
    "DEFAULT_ACLS": DEFAULT_ACLS
}
CONFIG = dict_merge(BASE_CONFIG, CONFIG)
CONFIG = dict_merge(KEYS, CONFIG)
