from copy import deepcopy
import logging
import os
import shutil
import urllib

import boto3
from boto3.dynamodb.conditions import Attr
import bson  # For IDs
from deriva.core import DerivaServer
import globus_sdk
import mdf_toolbox
import requests

from cfde_ap import CONFIG
from . import error as err
from cfde_deriva.datapackage import CfdeDataPackage


logger = logging.getLogger(__name__)

DMO_CLIENT = boto3.resource('dynamodb',
                            aws_access_key_id=CONFIG["AWS_KEY"],
                            aws_secret_access_key=CONFIG["AWS_SECRET"],
                            region_name="us-east-1")
DMO_SCHEMA = {
    "AttributeDefinitions": [{
        "AttributeName": "action_id",
        "AttributeType": "S"
    }],
    "KeySchema": [{
        "AttributeName": "action_id",
        "KeyType": "HASH"
    }],
    "ProvisionedThroughput": {
        "ReadCapacityUnits": 20,
        "WriteCapacityUnits": 20
    }
}


def clean_environment():
    # Delete data dir and remake
    try:
        shutil.rmtree(CONFIG["DATA_DIR"])
    except FileNotFoundError:
        pass
    os.makedirs(CONFIG["DATA_DIR"])
    # Clear old exceptional error log
    try:
        os.remove("ERROR.log")
    except FileNotFoundError:
        pass


def initialize_dmo_table(table_name, schema=DMO_SCHEMA, client=DMO_CLIENT):
    """Init a table in DynamoDB, by default the DMO_TABLE with DMO_SCHEMA.
    Currently not intended to be called in a script;
    table creation is only necessary once per table.

    Arguments:
        table_name (str): The name for the DynamoDB table.
        schema (dict): The schema for the DynamoDB table.
                Default DMO_SCHEMA.
        client (dynamodb.ServiceResource): An authenticated client for DynamoDB.
                Default DMO_CLIENT.

    Returns:
        dynamodb.Table: The created DynamoDB table.

    Raises exception on any failure.
    """
    # Table should not be active already
    try:
        get_dmo_table(table_name, client)
    except err.NotFound:
        pass
    else:
        raise err.InvalidState("Table already created")

    schema = deepcopy(DMO_SCHEMA)
    schema["TableName"] = table_name

    try:
        new_table = client.create_table(**schema)
        new_table.wait_until_exists()
    except client.meta.client.exceptions.ResourceInUseException:
        raise err.InvalidState("Table concurrently created")
    except Exception as e:
        raise err.ServiceError(str(e))

    # Check that table now exists
    try:
        table2 = get_dmo_table(table_name, client)
    except err.NotFound:
        raise err.InternalError("Unable to create table")

    return table2


def get_dmo_table(table_name, client=DMO_CLIENT):
    """Return a DynamoDB table, by default the DMO_TABLE.

    Arguments:
        table_name (str): The name of the DynamoDB table.
        client (dynamodb.ServiceResource): An authenticated client for DynamoDB.
                Default DMO_CLIENT.

    Returns:
        dynamodb.Table: The requested DynamoDB table.

    Raises exception on any failure.
    """
    try:
        table = client.Table(table_name)
        dmo_status = table.table_status
        if dmo_status != "ACTIVE":
            raise ValueError("Table not active")
    except (ValueError, client.meta.client.exceptions.ResourceNotFoundException):
        raise err.NotFound("Table does not exist or is not active")
    except Exception as e:
        raise err.ServiceError(str(e))
    else:
        return table


def generate_action_id(table_name):
    """Generate a valid action_id, unique to the given table.

    Arguments:
        table_name (str): The name of the table to check uniqueness against.

    Returns:
        str: The action_id.
    """
    # TODO: Different ID generation logic?
    action_id = str(bson.ObjectId())
    while True:
        try:
            read_action_status(table_name, action_id)
        except err.NotFound:
            break
        else:
            action_id = str(bson.ObjectId())
    return action_id


def create_action_status(table_name, action_status):
    """Create action entry in status database (DynamoDB).

    Arguments:
        table_name (str): The name of the DynamoDB table.
        action_status (dict): The initial status for the action.

    Returns:
        dict: The action status created (including action_id).

    Raises exception on any failure.
    """
    table = get_dmo_table(table_name)

    # TODO: Add default status information
    action_id = generate_action_id(table_name)
    action_status["action_id"] = action_id
    if not action_status.get("details"):
        action_status["details"] = {
            "message": "Action started"
        }

    # TODO: Validate entry
    status_errors = []
    if status_errors:
        raise err.InvalidRequest(*status_errors)

    # Push to Dynamo table
    try:
        table.put_item(Item=action_status, ConditionExpression=Attr("action_id").not_exists())
    except Exception as e:
        logger.error("Error creating status for '{}': {}".format(action_id, str(e)))
        raise err.ServiceError(str(e))

    logger.info("{}: Action status created".format(action_id))
    return action_status


def read_action_status(table_name, action_id):
    """Fetch an action entry from status database.

    Arguments:
        table_name (str): The name of the table to read from.
        action_id (dict): The ID for the action.

    Returns:
        dict: The requested action status.

    Raises exception on any failure.
    """
    table = get_dmo_table(table_name)

    # If not found, Dynamo will return empty, only raising error on service issue
    try:
        entry = table.get_item(Key={"action_id": action_id}, ConsistentRead=True).get("Item")
    except Exception as e:
        logger.error("Error reading status for '{}': {}".format(action_id, str(e)))
        raise err.ServiceError(str(e))

    if not entry:
        raise err.NotFound("Action ID {} not found in status database".format(action_id))
    return entry


def read_action_by_request(table_name, request_id):
    """Fetch an action entry given its request_id instead of action_id.
    This requires scanning the DynamoDB table.

    Arguments:
        table_name (str): The name of the table to read from.
        request_id (str): The requested request_id.

    Returns:
        dict: The requested action status.

    Raises exception on any failure.
    """
    table = get_dmo_table(table_name)

    scan_args = {
        "ConsistentRead": True,
        "FilterExpression": Attr("request_id").eq(request_id)
    }
    # Make scan call, paging through if too many entries are scanned
    result_entries = []
    while True:
        scan_res = table.scan(**scan_args)
        # Check for success
        if scan_res["ResponseMetadata"]["HTTPStatusCode"] >= 300:
            logger.error("Scan error: {}: {}"
                         .format(scan_res["ResponseMetadata"]["HTTPStatusCode"],
                                 scan_res["ResponseMetadata"]))
            raise err.ServiceError(scan_res["ResponseMetadata"])
        # Add results to list
        result_entries.extend(scan_res["Items"])
        # Check for completeness
        # If LastEvaluatedKey exists, need to page through more results
        if scan_res.get("LastEvaluatedKey", None) is not None:
            scan_args["ExclusiveStartKey"] = scan_res["LastEvaluatedKey"]
        # Otherwise, all results retrieved
        else:
            break

    # Should be exactly 0 or 1 result, 2+ should never happen
    if len(result_entries) <= 0:
        raise err.NotFound("Request ID '{}' not found in status database".format(request_id))
    elif len(result_entries) == 1:
        return result_entries[0]
    else:
        logger.error("Multiple entries found for request ID '{}'!".format(request_id))
        raise err.InternalError("Multiple entries found for request ID '{}'. "
                                "Please report this error.".format(request_id))


def update_action_status(table_name, action_id, updates, overwrite=False):
    """Update action entry in status database.

    Arguments:
        table_name (str): The name of the table to update.
        action_id (dict): The ID for the action.
        updates (dict): The updates to apply to the action status.
        overwrite (bool): When False, will merge the updates into the existing status,
                overwriting only existing values.
                When True, will delete the existing status entirely and replace it
                with the updates.
                Default False.

    Returns:
        dict: The updated action status.

    Raises exception on any failure.
    """
    # Verify old status exists and save it
    old_status = read_action_status(table_name, action_id)

    # Merge updates into old_status if not overwriting
    if not overwrite:
        # dict_merge(base, addition) returns base keys unchanged, addition keys added
        full_updates = mdf_toolbox.dict_merge(updates, old_status)
    else:
        full_updates = updates

    # TODO: Validate updates
    update_errors = []
    if update_errors:
        raise err.InvalidRequest(*update_errors)

    # Update in DB (.put_item() overwrites)
    table = get_dmo_table(table_name)
    try:
        table.put_item(Item=full_updates)
    except Exception as e:
        logger.error("Error updating status for '{}': {}".format(action_id, str(e)))
        raise err.ServiceError(str(e))

    logger.debug("{}: Action status updated: {}".format(action_id, updates))
    return full_updates


def delete_action_status(table_name, action_id):
    """Release an action entry from the database.

    Arguments:
        table_name (str): The name of the table to delete from.
        action_id (dict): The ID for the action.

    Raises exception on any failure.
    """
    # Check that entry exists currently
    # Throws exceptions if it doesn't exist
    read_action_status(table_name, action_id)

    table = get_dmo_table(table_name)

    # Delete entry
    try:
        table.delete_item(Key={"action_id": action_id})
    except Exception as e:
        logger.error("Error deleting status for '{}': {}".format(action_id, str(e)))
        err.ServiceError(str(e))

    # Verify deletion
    try:
        read_action_status(table_name, action_id)
    except err.NotFound:
        pass
    else:
        logger.error("{} error: Action status in database after deletion".format(action_id))
        raise err.InternalError("Action status was not deleted.")

    logger.info("{}: Action status deleted".format(action_id))
    return


def translate_status(raw_status):
    """Translate raw status into user-servable form.

    Arguments:
        raw_status (dict): The status from the database to translate.

    Returns:
        dict: The translated status.
    """
    # TODO
    # DynamoDB stores int as Decimal, which isn't JSON-friendly
    if raw_status.get("details", {}).get("deriva_id"):
        raw_status["details"]["deriva_id"] = int(raw_status["details"]["deriva_id"])
    return raw_status


def get_deriva_token():
    # TODO: When decision is made about user auth vs. conf client auth, implement.
    #       Currently using personal refresh token for scope.
    #       Refresh token will expire in six months(?)
    #       Date last generated: 9-26-2019

    return globus_sdk.RefreshTokenAuthorizer(
                        refresh_token=CONFIG["TEMP_REFRESH_TOKEN"],
                        auth_client=globus_sdk.NativeAppAuthClient(CONFIG["GLOBUS_NATIVE_APP"])
           ).access_token


def _generate_new_deriva_token():
    # Generate new Refresh Token to be used in get_deriva_token()
    native_client = globus_sdk.NativeAppAuthClient(CONFIG["GLOBUS_NATIVE_APP"])
    native_flow = native_client.oauth2_start_flow(
                                    requested_scopes=("https://auth.globus.org/scopes/demo."
                                                      "derivacloud.org/deriva_all"),
                                    refresh_tokens=True)
    code = input(f"Auth at '{native_flow.get_authorize_url()}' and paste code:\n")
    tokens = native_flow.exchange_code_for_tokens(code)
    return tokens["refresh_token"]


def download_data(source_loc, local_path):
    """Download data from a remote host to the configured machine.
    (Many sources to one destination)

    Arguments:
        source_loc (list of str): The location(s) of the data.
        local_path (str): The path to the local storage location.

    Returns:
        dict: success (bool): True on success, False on failure.
    """
    filename = None
    # If the local_path is a file and not a directory, use the directory
    if ((os.path.exists(local_path) and not os.path.isdir(local_path))
            or (not os.path.exists(local_path) and local_path[-1] != "/")):
        # Save the filename for later
        filename = os.path.basename(local_path)
        local_path = os.path.dirname(local_path) + "/"

    os.makedirs(local_path, exist_ok=True)
    if not isinstance(source_loc, list):
        source_loc = [source_loc]

    # Download data locally
    for location in source_loc:
        loc_info = urllib.parse.urlparse(location)
        # HTTP(S)
        if loc_info.scheme.startswith("http"):
            # Get default filename and extension
            http_filename = os.path.basename(loc_info.path)
            if not http_filename:
                http_filename = "archive"
            ext = os.path.splitext(http_filename)[1]
            if not ext:
                ext = ".archive"

            # Fetch file
            res = requests.get(location)
            # Get filename from header if present
            con_disp = res.headers.get("Content-Disposition", "")
            filename_start = con_disp.find("filename=")
            if filename_start >= 0:
                filename_end = con_disp.find(";", filename_start)
                if filename_end < 0:
                    filename_end = None
                http_filename = con_disp[filename_start+len("filename="):filename_end]
                http_filename = http_filename.strip("\"'; ")

            # Create path for file
            archive_path = os.path.join(local_path, filename or http_filename)
            # Make filename unique if filename is duplicate
            collisions = 0
            while os.path.exists(archive_path):
                # Save and remove extension
                archive_path, ext = os.path.splitext(archive_path)
                old_add = "({})".format(collisions)
                collisions += 1
                new_add = "({})".format(collisions)
                # If added number already, remove before adding new number
                if archive_path.endswith(old_add):
                    archive_path = archive_path[:-len(old_add)]
                # Add "($num_collisions)" to end of filename to make filename unique
                archive_path = archive_path + new_add + ext

            # Download and save file
            with open(archive_path, 'wb') as out:
                out.write(res.content)
            logger.debug("Downloaded HTTP file: {}".format(archive_path))
        # Not supported
        else:
            # Nothing to do
            raise IOError("Invalid data location: '{}' is not a recognized protocol "
                          "(from {}).".format(loc_info.scheme, str(location)))

    # Extract all archives, delete extracted archives
    extract_res = mdf_toolbox.uncompress_tree(local_path, delete_archives=True)
    if not extract_res["success"]:
        raise IOError("Unable to extract archives in dataset")

    return {
        "success": True,
        "num_extracted": extract_res["num_extracted"],
        "total_files": sum([len(files) for _, _, files in os.walk(local_path)])
    }


def deriva_ingest(servername, data_json_file, catalog_id=None, acls=None):
    """Perform an ingest to DERIVA into a catalog, using the CfdeDataPackage.

    Arguments:
        servername (str): The name of the DERIVA server.
        data_json_file (str): The path to the JSON file with TableSchema data.
        catalog_id (str): If updating an existing catalog, the existing catalog ID.
                Default None, to create a new catalog.
        acls (dict): The ACLs to set on the catalog. Currently nonfunctional.
                Default None.

    Returns:
        dict: The result of the ingest.
            success (bool): True when the ingest was successful.
            catalog_id (str): The catalog's ID.
    """
    datapack = CfdeDataPackage(data_json_file, verbose=False)
    # Format credentials in DerivaServer-expected format
    creds = {
        "bearer-token": get_deriva_token()
    }
    server = DerivaServer("https", servername, creds)
    if catalog_id:
        catalog = server.connect_ermrest(catalog_id)
    else:
        catalog = server.create_ermrest_catalog()
    datapack.set_catalog(catalog)
    if not catalog_id:
        datapack.provision()
    # datapack.apply_acls(acls)
    datapack.load_data_files()

    return {
        "success": True,
        "catalog_id": catalog.catalog_id
    }
