INPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "Deriva Demo Input",
    "description": "Input schema for the demo Deriva ingest Action Provider.",
    "type": "object",
    "properties": {
        "url": {
            "type": "string",
            "format": "uri",
            "description": "The URL of the DERIVA restore data."
        },
        "catalog": {
            "type": "string",
            "description": "The DERIVA catalog to restore into."
        }
    },
    "required": [
        "url"
    ]
}

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
