import os


BASE_CONFIG = {
    "GLOBUS_NATIVE_APP": "417301b1-5101-456a-8a27-423e71a2ae26",
    "GLOBUS_CC_APP": "21017803-059f-4a9b-b64c-051ab7c1d05d",
    "GLOBUS_SCOPE": "https://auth.globus.org/scopes/21017803-059f-4a9b-b64c-051ab7c1d05d/demo",
    "DEPENDENT_SCOPES": {
        "deriva_all": {
            "https://auth.globus.org/scopes/app.nih-cfde.org/deriva_all"
        },
        "transfer": {
            "urn:globus:auth:scope:transfer.api.globus.org:all",
        }
    },
    "LONG_TERM_STORAGE": '/CFDE/public/',
    "GLOBUS_AUD": "cfde_ap_demo",
    "GLOBUS_GROUP": "a437abe3-c9a4-11e9-b441-0efb3ba9a670",
    "ALLOWED_GCS_HTTPS_HOSTS": r"https://[^/]*[.]data[.]globus[.]org/.*",
    "DATA_DIR": os.path.join(os.path.expanduser("~"), "deriva_data"),
    "DERIVA_SCHEMA_NAME": "CFDE",
    "TRANSFER_PING_INTERVAL": 60,  # Seconds
    "TRANSFER_DEADLINE": 24 * 60 * 60,  # 1 day, in seconds
    "LOGGING": {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "basic": {
                "format": "[{asctime}] [{levelname}] {name}.{funcName}-{processName}: {message}",
                "style": "{",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "DEBUG",
                "formatter": "basic",
            },
            "logfile": {
                #
                "class": "logging.NullHandler",
                # FileHandler does not play well with systemd. Use NullHandler
                # instead for deployments
                # "class": "logging.FileHandler",
                # "level": "DEBUG",
                # "mode": "a",
                # "filename": "api.log",
                # "formatter": "basic",
            }
        },
        "loggers": {
            "cfde_ap": {"level": "DEBUG", "handlers": ["console", "logfile"]},
            "cfde_deriva": {"level": "DEBUG", "handlers": ["console", "logfile"]},
            "bdbag": {"level": "DEBUG", "handlers": ["console", "logfile"]},
        },
        # use "ROOT" log level handler for detailed log dumps of EVERYTHING. Use
        # the above for basic logging in only select modules. "root" propogates
        # much more useless info for debugging. Don"t use both.
        # "root": {
        #     "level": "DEBUG",
        #     "handlers": ["console", "logfile"]
        # },
    }
}
