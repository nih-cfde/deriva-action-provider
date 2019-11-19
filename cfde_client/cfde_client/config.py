import globus_automate_client


CONFIG = {
    # Translations for Automate states into nicer language
    "STATE_MSGS": {
        "ACTIVE": "is still in progress",
        "INACTIVE": "has stalled, and may need help to resume",
        "SUCCEEDED": "has completed successfully",
        "FAILED": "has failed"
    },
    # Translations for known catalogs into schemas
    "KNOWN_CATALOGS": {
        "demo": "demo",
        "prod": "prod",
        "stage": "stage",
        "dev": "dev"
    },
    # Automate Scopes and Flows
    "HTTPS_SCOPE": "https://auth.globus.org/scopes/0e57d793-f1ac-4eeb-a30f-643b082d68ec/https",
    "AUTOMATE_SCOPES": list(globus_automate_client.flows_client.ALL_FLOW_SCOPES),
    "TRANSFER_FLOW": "b1e13f5e-dad6-4524-8c3f-fb39e752a266",
    "HTTP_FLOW": "056c26d7-3bf4-4022-8b4d-029875b5e8c0",
    # FAIR Research Endpoint destination directory and HTTPS URL
    "EP_DIR": "/public/CFDE/metadata/",
    "EP_URL": "https://317ec.36fe.dn.glob.us",
    # Format for BDBag archives
    "ARCHIVE_FORMAT": "tgz"
}
# Add all necessary scopes together for Auth call
CONFIG["ALL_SCOPES"] = CONFIG["AUTOMATE_SCOPES"] + [CONFIG["HTTPS_SCOPE"]]
