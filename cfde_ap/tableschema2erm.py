"""Translate basic Frictionless Table-Schema table definitions to Deriva.

convert_tableschema(tableschema, schema_name = 'CFDE', skip_system_cols = False)
returns ERM schema as dictionary

"""

from deriva.core.ermrest_model import builtin_types, Table, Column, Key, ForeignKey

schema_tag = 'tag:isrd.isi.edu,2019:table-schema-leftovers'
resource_tag = 'tag:isrd.isi.edu,2019:table-resource'


def make_type(type, format):
    """Choose appropriate ERMrest column types..."""
    if type == "string":
        return builtin_types.text
    if type == "datetime":
        return builtin_types.timestamptz
    if type == "date":
        return builtin_types.date
    if type == "integer":
        return builtin_types.int8
    if type == "number":
        return builtin_types.float8
    if type == "list":
        # assume a list is a list of strings for now...
        return builtin_types["text[]"]
    raise ValueError('no mapping defined yet for type=%s format=%s' % (type, format))


def make_column(cdef):
    cdef = dict(cdef)
    constraints = cdef.get("constraints", {})
    cdef_name = cdef.pop("name")
    nullok = not constraints.pop("required", False)
    description = cdef.pop("description", None)
    return Column.define(
        cdef_name,
        make_type(
            cdef.get("type", "string"),
            cdef.get("format", "default"),
        ),
        nullok=nullok,
        comment=description,
        annotations={
            schema_tag: cdef,
        }
    )


def make_key(tname, cols, schema_name):
    return Key.define(
        cols,
        constraint_names=[[schema_name, "%s_%s_key" % (tname, "_".join(cols))]],
    )


def make_fkey(tname, fkdef, schema_name):
    fkcols = fkdef.pop("fields")
    fkcols = [fkcols] if isinstance(fkcols, str) else fkcols
    reference = fkdef.pop("reference")
    pktable = reference.pop("resource")
    pktable = tname if pktable == "" else pktable
    pkcols = reference.pop("fields")
    pkcols = [pkcols] if isinstance(pkcols, str) else pkcols
    return ForeignKey.define(
        fkcols,
        schema_name,
        pktable,
        pkcols,
        constraint_names=[[schema_name, "%s_%s_fkey" % (tname, "_".join(fkcols))]],
        annotations={
            schema_tag: fkdef,
        }
    )


def make_table(tdef, schema_name, skip_system_cols=False):
    tname = tdef["name"]
    tcomment = tdef.get("description")
    tdef_resource = tdef
    tdef = tdef_resource.pop("schema")
    keys = []
    keysets = set()
    pk = tdef.pop("primaryKey", None)
    if isinstance(pk, str):
        pk = [pk]
    if isinstance(pk, list):
        keys.append(make_key(tname, pk, schema_name))
        keysets.add(frozenset(pk))
    tdef_fields = tdef.pop("fields", None)
    for cdef in tdef_fields:
        if cdef.get("constraints", {}).pop("unique", False):
            kcols = [cdef["name"]]
            if frozenset(kcols) not in keysets:
                keys.append(make_key(tname, kcols, schema_name))
                keysets.add(frozenset(kcols))
    tdef_fkeys = tdef.pop("foreignKeys", [])
    return Table.define(
        tname,
        column_defs=[
            make_column(cdef)
            for cdef in tdef_fields
        ],
        key_defs=keys,
        fkey_defs=[
            make_fkey(tname, fkdef, schema_name)
            for fkdef in tdef_fkeys
        ],
        comment=tcomment,
        provide_system=skip_system_cols,
        annotations={
            resource_tag: tdef_resource,
            schema_tag: tdef,
        }
    )


def convert_tableschema(tableschema, schema_name='CFDE', skip_system_cols=False):
    resources = tableschema['resources']
    deriva_schema = {
        "schemas": {
            schema_name: {
                "schema_name": schema_name,
                "tables": {
                    tdef["name"]: make_table(tdef, schema_name, skip_system_cols)
                    for tdef in resources
                }
            }
        }
    }
    return deriva_schema
