#!/usr/bin/env python3

import sys

from deriva.core import DerivaServer, get_credential

from cfde_datapackage import CfdeDataPackage

"""
Basic C2M2 catalog sketch

Demonstrates use of deriva-py APIs:
- server authentication (assumes active deriva-auth agent)
- catalog creation
- model provisioning
- basic configuration of catalog ACLs
- small Chaise presentation tweaks via model annotations
- simple insertion of tabular content

Examples:

   python3 ./examples/setup_c2m2_catalog.py ./table-schema/cfde-core-model.json

   python3 /path/to/GTEx.v7.C2M2_preload.bdbag/data/GTEx_C2M2_instance.json

when the JSON includes "path" attributes for the resources, as in the
second example above, the data files (TSV assumed) are loaded for each
resource after the schema is provisioned.

"""

# this is the deriva server where we will create a catalog
servername = 'demo.derivacloud.org'

# bind to server
credentials = get_credential(servername)
server = DerivaServer('https', servername, credentials)

# ugly quasi CLI...
if len(sys.argv) != 2:
    raise ValueError('One data package JSON filename required as argument')

# pre-load all JSON files and convert to models
# in order to abort early on basic usage errors
dp = CfdeDataPackage(sys.argv[1])


# create catalog
catalog = server.create_ermrest_catalog()
print('New catalog has catalog_id=%s' % catalog.catalog_id)
print("Don't forget to delete it if you are done with it!")

# deploy model(s)
dp.set_catalog(catalog)
dp.provision()
print("Model deployed for %s." % (dp.filename,))

# set acls
dp.apply_acls()

# load some sample data?
dp.load_data_files()

print("All data packages loaded.")

print("Try visiting 'https://%s/chaise/recordset/#%s/CFDE:dataset'" % (
    servername,
    catalog.catalog_id,
))

# catalog.delete_ermrest_catalog(really=True)
