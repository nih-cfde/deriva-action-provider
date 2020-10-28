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
        '''
        "fair_re_path": {
            "type": "string",
            "description": ("Path on the FAIR Research Examples endpoint, to support "
                            "Globus Transfer (with Automate) input.")
        },
        "restore": {
            "type": "boolean",
            "description": ("Whether or not this is a restoration of a backed-up catalog (true), "
                            "or an ingest of TableSchema data (false). When true, data_url "
                            "must point to a DERIVA backup. When false, data_url must point "
                            "to a BDBag of TableSchema data. The default is false.")
        },
        '''
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
        },
        "catalog_acls": {
            "type": "object",
            "description": ("The DERIVA permissions to apply to a new catalog. "
                            "If no ACLs are provided here and a new catalog is being created, "
                            "default ACLs will be used."),
            "properties": {
                "owner": {
                    "type": "array",
                    "description": "Formatted UUIDs for 'owner' permissions.",
                    "items": {
                        "type": "string",
                        "description": "One UUID"
                    }
                },
                "insert": {
                    "type": "array",
                    "description": "Formatted UUIDs for 'insert' permissions.",
                    "items": {
                        "type": "string",
                        "description": "One UUID"
                    }
                },
                "update": {
                    "type": "array",
                    "description": "Formatted UUIDs for 'update' permissions.",
                    "items": {
                        "type": "string",
                        "description": "One UUID"
                    }
                },
                "delete": {
                    "type": "array",
                    "description": "Formatted UUIDs for 'delete' permissions.",
                    "items": {
                        "type": "string",
                        "description": "One UUID"
                    }
                },
                "select": {
                    "type": "array",
                    "description": "Formatted UUIDs for 'select' permissions.",
                    "items": {
                        "type": "string",
                        "description": "One UUID"
                    }
                },
                "enumerate": {
                    "type": "array",
                    "description": "Formatted UUIDs for 'enumerate' permissions.",
                    "items": {
                        "type": "string",
                        "description": "One UUID"
                    }
                }
            }
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
