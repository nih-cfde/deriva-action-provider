import os
import json
import csv

from deriva.core import urlquote
from deriva.core.ermrest_config import tag

from . import tableschema2erm


# we'll use this utility function later...
def topo_sorted(depmap):
    """Return list of items topologically sorted.

       depmap: { item: [required_item, ...], ... }

    Raises ValueError if a required_item cannot be satisfied in any order.

    The per-item required_item iterables must allow revisiting on
    multiple iterations.

    """
    ordered = [item for item, requires in depmap.items() if not requires]
    depmap = {item: set(requires) for item, requires in depmap.items() if requires}
    satisfied = set(ordered)
    while depmap:
        additions = []
        for item, requires in list(depmap.items()):
            if requires.issubset(satisfied):
                additions.append(item)
                satisfied.add(item)
                del depmap[item]
        if not additions:
            raise ValueError(("unsatisfiable", depmap))
        ordered.extend(additions)
        additions = []
    return ordered


class CfdeDataPackage(object):
    # the translation stores frictionless table resource metadata under this annotation
    resource_tag = 'tag:isrd.isi.edu,2019:table-resource'
    # the translation leaves extranneous table-schema stuff under this annotation
    # (i.e. stuff that perhaps wasn't translated to deriva equivalents)
    schema_tag = 'tag:isrd.isi.edu,2019:table-schema-leftovers'

    def __init__(self, filename, verbose=True):
        self.filename = filename
        self.dirname = os.path.dirname(self.filename)
        self.catalog = None
        self.model_root = None
        self.cfde_schema = None
        self.verbose = verbose

        with open(self.filename, 'r') as f:
            tableschema = json.loads(f.read())

        self.model_doc = tableschema2erm.convert_tableschema(tableschema, 'CFDE', True)

        if set(self.model_doc['schemas']) != {'CFDE'}:
            raise NotImplementedError('Unexpected schema set in data package: '
                                      '%s' % (self.model_doc['schemas'],))

    def set_catalog(self, catalog):
        self.catalog = catalog
        self.get_model()

    def get_model(self):
        self.model_root = self.catalog.getCatalogModel()
        self.cfde_schema = self.model_root.schemas.get('CFDE')

    def provision(self):
        if 'CFDE' not in self.model_root.schemas:
            # blindly load the whole model on an apparently empty catalog
            self.catalog.post('/schema', json=self.model_doc).raise_for_status()
        else:
            # do some naively idempotent model definitions on existing catalog
            # adding missing tables and missing columns
            need_tables = []
            need_columns = []
            hazard_fkeys = {}
            for tname, tdoc in self.model_doc['schemas']['CFDE']['tables'].items():
                if tname in self.cfde_schema.tables:
                    table = self.cfde_schema.tables[tname]
                    for cdoc in tdoc['column_definitions']:
                        if cdoc['name'] in table.column_definitions.elements:
                            column = table.column_definitions.elements[cdoc['name']]
                            # TODO: check existing columns for compatibility?
                        else:
                            cdoc.update({'table_name': tname, 'nullok': True})
                            need_columns.append(cdoc)
                    # TODO: check existing table keys/foreign keys for compatibility?
                else:
                    tdoc['schema_name'] = 'CFDE'
                    need_tables.append(tdoc)

            if need_tables:
                if self.verbose:
                    print("Added tables %s" % ([tdoc['table_name'] for tdoc in need_tables]))
                self.catalog.post('/schema', json=need_tables).raise_for_status()

            for cdoc in need_columns:
                self.catalog.post(
                    '/schema/CFDE/table/%s/column' % urlquote(cdoc['table_name']),
                    json=cdoc
                ).raise_for_status()
                if self.verbose:
                    print("Added column %s.%s" % (cdoc['table_name'], cdoc['name']))

        self.get_model()

    def apply_acls(self, acls):
        self.get_model()
        self.model_root.acls.update(acls)

        # set custom chaise configuration values for this catalog
        self.model_root.annotations[tag.chaise_config] = {
            # hide system metadata by default in tabular listings, to focus on CFDE-specific content
            "SystemColumnsDisplayCompact": [],
        }

        # apply the above ACL and annotation changes to server
        self.model_root.apply(self.catalog)
        self.get_model()

    @classmethod
    def make_row2dict(cls, table, header):
        """Pickle a row2dict(row) function for use with a csv reader"""
        numcols = len(header)
        missingValues = set(table.annotations[cls.schema_tag].get("missingValues", []))

        for cname in header:
            if cname not in table.column_definitions.elements:
                raise ValueError("header column %s not found in table %s" % (cname, table.name))

        def row2dict(row):
            """Convert row tuple to dictionary of {col: val} mappings."""
            return dict(zip(
                header,
                [None if x in missingValues else x for x in row]
            ))

        return row2dict

    def data_tnames_topo_sorted(self):
        def target_tname(fkey):
            return fkey.referenced_columns[0]["table_name"]
        tables_doc = self.model_doc['schemas']['CFDE']['tables']
        return topo_sorted({
            table.name: [
                target_tname(fkey)
                for fkey in table.foreign_keys
                if target_tname(fkey) != table.name and target_tname(fkey) in tables_doc
            ]
            for table in self.cfde_schema.tables.values()
            if table.name in tables_doc
        })

    def load_data_files(self):
        tables_doc = self.model_doc['schemas']['CFDE']['tables']
        for tname in self.data_tnames_topo_sorted():
            # we are doing a clean load of data in fkey dependency order
            table = self.model_root.table("CFDE", tname)
            resource = tables_doc[tname]["annotations"].get(self.resource_tag, {})
            if "path" in resource:
                fname = "%s/%s" % (self.dirname, resource["path"])
                with open(fname, "r") as f:
                    # translate TSV to python dicts
                    reader = csv.reader(f, delimiter="\t")
                    raw_rows = list(reader)
                    row2dict = self.make_row2dict(table, raw_rows[0])
                    dict_rows = [row2dict(row) for row in raw_rows[1:]]
                    self.catalog.post("/entity/CFDE:%s" % urlquote(table.name), json=dict_rows)
                    if self.verbose:
                        print("Table %s data loaded from %s." % (table.name, fname))
