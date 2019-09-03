from mdf_toolbox import dict_merge

from .schemas import INPUT_SCHEMA, OUTPUT_SCHEMA
from .keys import KEYS
from .base_config import BASE_CONFIG

# Config setup
CONFIG = {
    "INPUT_SCHEMA": INPUT_SCHEMA,
    "OUTPUT_SCHEMA": OUTPUT_SCHEMA
}
CONFIG = dict_merge(BASE_CONFIG, CONFIG)
CONFIG = dict_merge(KEYS, CONFIG)
