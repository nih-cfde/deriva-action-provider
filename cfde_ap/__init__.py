from mdf_toolbox import dict_merge

from .keys import KEYS
from .base_config import BASE_CONFIG

# Config setup
CONFIG = {}
CONFIG = dict_merge(BASE_CONFIG, CONFIG)
CONFIG = dict_merge(KEYS, CONFIG)
