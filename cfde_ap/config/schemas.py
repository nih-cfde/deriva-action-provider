INPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "Deriva Demo Input",
    "description": ("Input schema for the demo DERIVA ingest Action Provider. This Action "
                    "Provider can restore a DERIVA backup to a new or existing catalog, "
                    "or create a new DERIVA catalog from a BDBag containing TableSchema."),
    "type": "object",
    "properties": {
        "data_url": {
            "type": "string",
            "format": "uri",
            "description": "The URL or path to the data for DERIVA ingest."
        },
        "globus_ep": {
            "type": "string",
            "description": ("The UUID of the Globus endpoint/collection for the data. "
                            "Only required if the data_url is a non-public Globus endpoint.")
        },
        "operation": {
            "type": "string",
            "description": ("The operation to perform on the data. If the data is a DERIVA backup "
                            "to restore, use 'restore'. If the data is TableSchema to ingest into "
                            "a new or existing DERIVA catalog, use 'ingest'. If you are only "
                            "modifying the parameters of one catalog, use 'modify'."),
            "enum": [
                "restore",
                "ingest",
                "modify"
            ]
        },
        "server": {
            "type": "string",
            "description": ("The DERIVA server to ingest into. By default, will use the DERIVA "
                            "demo server.")
        },
        "catalog_id": {
            "type": ["string", "integer"],
            "description": ("The existing catalog ID to ingest into, or the name of a pre-defined "
                            "catalog (e.g. 'prod'). To create a new catalog, do not specify "
                            "this value. If specified, the catalog must exist.")
        }
    },
    "required": ["operation"]
}

# TODO
OUTPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "Deriva Demo Output",
    "description": "Output schema for the demo Deriva ingest Action Provider.",
    "type": "object",
    "properties": {
    },
    "required": [
    ]
}
