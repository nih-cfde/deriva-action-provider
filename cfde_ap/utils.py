from copy import deepcopy
import logging

import boto3
from boto3.dynamodb.conditions import Attr
import bson  # For IDs
import mdf_toolbox

from cfde_ap import CONFIG
from . import error as err


logger = logging.getLogger(__name__)

DMO_CLIENT = boto3.resource('dynamodb',
                            aws_access_key_id=CONFIG["AWS_KEY"],
                            aws_secret_access_key=CONFIG["AWS_SECRET"],
                            region_name="us-east-1")
# DMO_TABLE = "cfde-demo-status"
DMO_SCHEMA = {
    # "TableName": DMO_TABLE,
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
    return raw_status
