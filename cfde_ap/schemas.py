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
            "description": "The URL to the data for DERIVA ingest."
        },
        "restore": {
            "type": "boolean",
            "description": ("Whether or not this is a restoration of a backed-up catalog (true), "
                            "or an ingest of TableSchema data (false). When true, data_url "
                            "must point to a DERIVA backup. When false, data_url must point "
                            "to a BDBag of TableSchema data. The default is false.")
        },
        "catalog_id": {
            "type": "string",
            "description": ("The existing catalog ID to ingest into. To create a new catalog, "
                            "do not specify this value. If specified, the catalog must exist.")
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
    "required": [
        "data_url"
    ]
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
