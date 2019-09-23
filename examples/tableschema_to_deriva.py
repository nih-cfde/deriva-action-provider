#!/usr/bin/env python3

"""Translate basic Frictionless Table-Schema table definitions to Deriva.

- Reads table-schema JSON on standard input
- Writes deriva schema JSON on standard output

The output JSON is suitable for POST to an /ermrest/catalog/N/schema
resource on a fresh, empty catalog.

Example:

   cd cfde-deriva
   python3 examples/tableschema_to_deriva.py \
     < table-schema/cfde-core-model.json

Optionally:

   run with SKIP_SYSTEM_COLUMNS=true to suppress generation of ERMrest
   system columns RID,RCT,RCB,RMT,RMB for each table.

"""

import os
import sys
import json
import tableschema2erm

tableschema = json.load(sys.stdin)
skip_system_cols = not (os.getenv('SKIP_SYSTEM_COLUMNS', 'false').lower() == 'true')
deriva_schema = tableschema2erm.convert_tableschema(tableschema, 'CFDE', skip_system_cols)
json.dump(deriva_schema, sys.stdout, indent=2)
