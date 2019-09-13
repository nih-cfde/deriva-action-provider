INPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "Deriva Demo Input",
    "description": ("Input schema for the demo DERIVA ingest Action Provider. This Action "
                    "Provider can restore a DERIVA backup to a new or existing catalog, "
                    "or create a new DERIVA catalog from a BDBag containing TableSchema."),
    "type": "object",
    "oneOf": [{
        "properties": {
            "restore_url": {
                "type": "string",
                "format": "uri",
                "description": "The URL of the DERIVA restore data, for a restore operation."
            },
            "restore_catalog": {
                "type": "string",
                "description": ("The DERIVA catalog to restore into. If a catalog is specified, "
                                "the catalog must already exist for the restore to succeed.")
            }
        },
        "required": [
            "restore_url"
        ]
    }, {
        "properties": {
            "ingest_url": {
                "type": "string",
                "format": "uri",
                "description": ("The URL to the BDBag containing TableSchema to ingest "
                                "into a new catalog.")
            },
            "ingest_catalog_acls": {
                "type": "object",
                "description": ("The DERIVA permissions to apply to the new catalog. "
                                "If no ACLs are provided here, default ones will be used."),
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
                },
                "required": [
                ]
            }
        },
        "required": [
            "ingest_url"
        ]
    }],
    "additionalProperties": False
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
