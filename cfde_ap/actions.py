import logging

from cfde_deriva.datapackage import CfdeDataPackage
from deriva.core import DerivaServer

from cfde_ap import CONFIG
from .utils import get_deriva_token


logger = logging.getLogger(__name__)


def deriva_ingest(servername, data_json_file, catalog_id=None, acls=None):
    """Perform an ingest to DERIVA into a catalog, using the CfdeDataPackage.

    Arguments:
        servername (str): The name of the DERIVA server.
        data_json_file (str): The path to the JSON file with TableSchema data.
        catalog_id (str or int): If updating an existing catalog, the existing catalog ID.
                Default None, to create a new catalog.
        acls (dict): The ACLs to set on the catalog.
                Default None to use default ACLs.

    Returns:
        dict: The result of the ingest.
            success (bool): True when the ingest was successful.
            catalog_id (str): The catalog's ID.
    """
    # Format credentials in DerivaServer-expected format
    creds = {
        "bearer-token": get_deriva_token()
    }
    # Get server object
    server = DerivaServer("https", servername, creds)

    # If ingesting into existing catalog, don't need to provision with schema
    if catalog_id:
        catalog_id = str(int(catalog_id))
        catalog = server.connect_ermrest(catalog_id)
    # Otherwise, we need to fetch the latest model for provisioning
    else:
        logger.debug("Provisioning new catalog")
        '''
        canon_schema = requests.get(CONFIG["DERIVA_SCHEMA_LOCATION"]).json()
        with tempfile.TemporaryDirectory() as schema_dir:
            schema_path = os.path.join(schema_dir, "model.json")
            with open(schema_path, 'w') as f:
                json.dump(canon_schema, f)
        '''
        try:
            provisional_datapack = CfdeDataPackage(CONFIG["DERIVA_SCHEMA_LOCATION"])
            catalog = server.create_ermrest_catalog()
            provisional_datapack.set_catalog(catalog)
            provisional_datapack.provision()
            provisional_datapack.load_data_files()
        except Exception:
            # On any exception, delete new catalog if possible, then continue with exception
            try:
                catalog.delete_ermrest_catalog(really=True)
            except Exception as e:
                logger.error(f"Unable to delete catalog {catalog.catalog_id}: {repr(e)}")
            raise

    try:
        # Now we create a datapackage to ingest the actual data
        logger.debug("Creating CfdeDataPackage")
        datapack = CfdeDataPackage(data_json_file)
        # Catalog was created previously
        datapack.set_catalog(catalog)

        # Apply custom config (if possible - may fail if non-canon schema)
        logger.debug("Applying custom config")
        try:
            datapack.apply_custom_config()
        # Using non-canon schema is not failure unless Deriva rejects data
        except Exception:
            logger.info(f"Custom config skipped for {catalog.catalog.id}")

        # Apply ACLs - either supplied or CfdeDataPackage default
        # Defaults are set in .apply_custom_config(), which can fail
        logger.debug("Applying ACLS")
        if acls is None:
            acls = dict(CfdeDataPackage.catalog_acls)
        # Ensure catalog owner still in ACLs - DERIVA forbids otherwise
        acls['owner'] = list(set(acls['owner']).union(datapack.cat_model_root.acls['owner']))
        # Apply acls
        datapack.cat_model_root.acls.update(acls)
        # Set ERMrest access
        datapack.cat_model_root.table('public', 'ERMrest_Client').acls\
                .update(datapack.ermrestclient_acls)
        datapack.cat_model_root.table('public', 'ERMrest_Group').acls\
                .update(datapack.ermrestclient_acls)
        # Submit changes to server
        datapack.cat_model_root.apply()

        # Load data from files into DERIVA
        # This is the step that will fail if the data are incorrect
        logger.debug("Loading data into DERIVA")
        datapack.load_data_files()
    except Exception:
        # On any exception, if this is a new catalog, delete the catalog if possible,
        # then continue raising original exception
        if not catalog_id:
            try:
                catalog.delete_ermrest_catalog(really=True)
            except Exception as e:
                logger.error(f"Unable to delete catalog {catalog.catalog_id}: {repr(e)}")
        raise

    return {
        "success": True,
        "catalog_id": catalog.catalog_id
    }


def deriva_modify(servername, catalog_id, acls=None):
    """Modify a DERIVA catalog's options using the CfdeDataPackage.
    Currently limited to ACL changes only.

    Arguments:
        servername (str): The name of the DERIVA server
        catalog_id (str or int): The ID of the catalog to change. The catalog must exist.
        acls (dict): The Access Control Lists to set.

    Returns:
        dict: The results of the update.
            success (bool): True if the ACLs were successfully changed.
    """
    catalog_id = str(int(catalog_id))
    # Format credentials in DerivaServer-expected format
    creds = {
        "bearer-token": get_deriva_token()
    }
    # Get the catalog model object to modify
    server = DerivaServer("https", servername, creds)
    catalog = server.connect_ermrest(catalog_id)
    cat_model = catalog.getCatalogModel()

    # If modifying ACL, set ACL
    if acls:
        # Ensure catalog owner still in ACLs - DERIVA forbids otherwise
        acls['owner'] = list(set(acls['owner']).union(cat_model.acls['owner']))
        # Apply acls
        cat_model.acls.update(acls)

    # Submit changes to server
    cat_model.apply()

    return {
        "success": True
    }
