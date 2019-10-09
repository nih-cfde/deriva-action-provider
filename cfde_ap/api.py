from datetime import datetime, timedelta, timezone
# import json
import logging
import multiprocessing
import os
import subprocess

from flask import Flask, jsonify, request
from globus_action_provider_tools.authentication import TokenChecker
from globus_action_provider_tools.validation import (
    request_validator,
    response_validator
)
from isodate import duration_isoformat, parse_duration, parse_datetime
import jsonschema
from openapi_core.wrappers.flask import FlaskOpenAPIResponse, FlaskOpenAPIRequest
import requests

from cfde_ap import CONFIG
from . import error as err, utils


# Flask setup
app = Flask(__name__)
app.config.from_mapping(**CONFIG)
app.url_map.strict_slashes = False

# Logging setup
# logger = logging.getLogger("cfde_ap")
logger = multiprocessing.get_logger()
logger.setLevel(CONFIG["LOG_LEVEL"])
logger.propagate = False
logfile_formatter = logging.Formatter("[{asctime}] [{levelname}] {name}: {message}",
                                      style='{',
                                      datefmt="%Y-%m-%d %H:%M:%S")
logfile_handler = logging.FileHandler(CONFIG["API_LOG_FILE"], mode='a')
logfile_handler.setFormatter(logfile_formatter)

logger.addHandler(logfile_handler)

logger.info("\n\n==========CFDE Action Provider started==========\n")

# Globals specific to this instance
TBL = CONFIG["DEMO_DYNAMO_TABLE"]
ROOT = "/"  # Segregate different APs by root path?
TOKEN_CHECKER = TokenChecker(CONFIG["GLOBUS_CC_APP"], CONFIG["GLOBUS_SECRET"],
                             [CONFIG["GLOBUS_SCOPE"]], CONFIG["GLOBUS_AUD"])

# Clean up environment
utils.clean_environment()


#######################################
# Flask helpers
#######################################

@app.errorhandler(err.ApiError)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status
    return response


@app.before_request
def before_request():
    # Service alive check can skip validation
    if request.path == "/ping":
        return {"success": True}
    wrapped_req = FlaskOpenAPIRequest(request)
    validation_result = request_validator.validate(wrapped_req)
    if validation_result.errors:
        raise err.InvalidRequest("; ".join([str(err) for err in validation_result.errors]))
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    auth_state = TOKEN_CHECKER.check_token(token)
    if not auth_state.identities:
        # Return auth errors for debugging - may change in prod for security
        raise err.NoAuthentication("; ".join([str(err) for err in auth_state.errors]))
    request.auth = auth_state


@app.after_request
def after_request(response):
    wrapped_req = FlaskOpenAPIRequest(request)
    wrapped_resp = FlaskOpenAPIResponse(response)
    validation_result = response_validator.validate(wrapped_req, wrapped_resp)
    if validation_result.errors:
        logger.error("Error on response: {}, {}"
                     .format(response.response, validation_result.errors))
        raise err.DeveloperError("; ".join([str(err) for err in validation_result.errors]))
    return response


#######################################
# API Routes
#######################################

@app.route(ROOT, methods=["GET"])
def meta():
    resp = {
        "types": ["Action"],
        "api_version": "1.0",
        "globus_auth_scope": CONFIG["GLOBUS_SCOPE"],
        "title": "CFDE Demo Deriva Ingest",
        "subtitle": ("A Globus Automate Action Provider to demonstrate ingestion "
                     "of a properly-formatted BDBag into DERIVA."),
        # "description": "",
        # "keywords": [],
        "visible_to": ["all_authenticated_users"],
        "runnable_by": ["urn:globus:groups:id:" + CONFIG["GLOBUS_GROUP"]],
        # "administered_by": [],
        # "admin_contact": "",
        "synchronous": False,
        "log_supported": False,
        # "maximum_deadline": "",
        "input_schema": CONFIG["INPUT_SCHEMA"],
        # "event_types": [],  # Event-type providers only
    }
    if not request.auth.check_authorization(resp["visible_to"],
                                            allow_all_authenticated_users=True):
        raise err.NotAuthorized("You cannot view this Action Provider.")
    return jsonify(resp)


@app.route(ROOT+"run", methods=["POST"])
def run():
    req = request.get_json(force=True)
    # Validate input
    body = req.get("body", {})
    try:
        jsonschema.validate(body, CONFIG["INPUT_SCHEMA"])
    except jsonschema.ValidationError as e:
        # Raise just the first line of the exception text, which contains the error
        # The entire body and schema are in the exception, which are too verbose
        raise err.InvalidRequest(str(e).split("\n")[0])
    # If request_id has been submitted before, return status instead of starting new
    try:
        status = utils.read_action_by_request(TBL, req["request_id"])
    # Otherwise, create new action
    except err.NotFound:
        # TODO: Accurately estimate completion time
        estimated_completion = datetime.now(tz=timezone.utc) + timedelta(days=1)

        default_release_after = timedelta(days=30)
        job = {
            # Start job as ACTIVE - no "waiting" status
            "status": "ACTIVE",
            # Default these to the principals of whoever is running this action:
            "manage_by": request.auth.identities,
            "monitor_by": request.auth.identities,
            "creator_id": request.auth.effective_identity,
            "release_after": default_release_after,
            "request_id": req["request_id"]
        }
        if "label" in req:
            job["label"] = req["label"]
        # Allow overriding by the request:
        if "manage_by" in req:
            job["manage_by"] = req["manage_by"]
        if "monitor_by" in req:
            job["monitor_by"] = req["monitor_by"]
        if "release_after" in req:
            job["release_after"] = parse_duration(req["release_after"]).tdelta
        if "deadline" in req:
            deadline = parse_datetime(req["deadline"])
            if deadline < estimated_completion:
                raise err.InvalidRequest(
                    f"Processing likely to exceed deadline of {req['deadline']}"
                )
        # Correct types for JSON serialization and DynamoDB ingest
        if isinstance(job["manage_by"], str):
            job["manage_by"] = [job["manage_by"]]
        else:
            job["manage_by"] = list(job["manage_by"])
        if isinstance(job["monitor_by"], str):
            job["monitor_by"] = [job["monitor_by"]]
        else:
            job["monitor_by"] = list(job["monitor_by"])
        # Standardize datetime to ISO format
        job["release_after"] = duration_isoformat(job["release_after"])

        # Create status in database (creates action_id)
        job = utils.create_action_status(TBL, job)

        # start_action() blocks, throws exception on failure, returns on success
        start_action(job["action_id"], req["body"])

        res = jsonify(utils.translate_status(job))
        res.status_code = 202
        return res
    else:
        return jsonify(utils.translate_status(status))


@app.route(ROOT+"<action_id>/status", methods=["GET"])
def status(action_id):
    status = utils.read_action_status(TBL, action_id)
    if not request.auth.check_authorization(status["monitor_by"]):
        raise err.NotAuthorized("You cannot view the status of action {}".format(action_id))
    return jsonify(utils.translate_status(status))


@app.route(ROOT+"<action_id>/cancel", methods=["POST"])
def cancel(action_id):
    status = utils.read_action_status(TBL, action_id)
    if not request.auth.check_authorization(status["manage_by"]):
        raise err.NotAuthorized("You cannot cancel action {}".format(action_id))

    clean_status = utils.translate_status(status)
    if clean_status["status"] in ["SUCCEEDED", "FAILED"]:
        raise err.InvalidState("Action {} already completed".format(action_id))

    cancel_action(action_id)
    new_status = utils.read_action_status(TBL, action_id)
    return jsonify(utils.translate_status(new_status))


@app.route(ROOT+"<action_id>/release", methods=["POST"])
def release(action_id):
    status = utils.read_action_status(TBL, action_id)
    if not request.auth.check_authorization(status["manage_by"]):
        raise err.NotAuthorized("You cannot cancel action {}".format(action_id))

    clean_status = utils.translate_status(status)
    if clean_status["status"] in ["ACTIVE", "INACTIVE"]:
        raise err.InvalidState("Action {} not completed and cannot be released".format(action_id))

    utils.delete_action_status(TBL, action_id)
    return clean_status


#######################################
# Synchronous events
#######################################

def start_action(action_id, action_data):
    # Restore Action
    if action_data.get("restore"):
        logger.info(f"{action_id}: Starting Deriva restore into "
                    f"{action_data.get('catalog_id', 'new catalog')}")
        # Spawn new process
        # TODO: Process management
        #       Currently assuming process manages itself
        args = (action_id, action_data["data_url"], action_data.get("catalog_id"))
        driver = multiprocessing.Process(target=action_restore, args=args, name=action_id)
        driver.start()
    # Ingest Action
    else:
        logger.info(f"{action_id}: Starting Deriva ingest into "
                    f"{action_data.get('catalog_id', 'new catalog')}")
        # Spawn new process
        args = (action_id, action_data["data_url"], action_data.get("catalog_id"),
                action_data.get("catalog_acls"))
        driver = multiprocessing.Process(target=action_ingest, args=args, name=action_id)
        driver.start()
    return


def cancel_action(action_id):
    # This action doesn't implement cancellation,
    # which is valid according to the Automate spec.
    # This is a stub in case cancellation is implemented later.
    return


#######################################
# Asynchronous actions
#######################################

def action_restore(action_id, url, catalog=None):
    token = utils.get_deriva_token()

    # Download backup zip file
    # TODO: Determine file type
    #       Use original file name (Content-Disposition)
    #       Make filename unique if collision

    # Excessive try-except blocks because there's (currently) no process management;
    # if the action fails, it needs to always self-report failure

    logger.debug(f"{action_id}: Deriva restore process started")
    # Setup
    try:
        file_path = os.path.join(CONFIG["DATA_DIR"], "cfde-backup.zip")
    except Exception as e:
        error_status = {
            "status": "FAILED",
            "details": {
                "error": "Error in action setup: " + str(e)
            }
        }
        # If update fails, last-ditch effort is write to error file for debugging
        try:
            utils.update_action_status(TBL, action_id, error_status)
        except Exception as e2:
            with open("ERROR.log", 'w') as out:
                out.write(f"Error updating status on {action_id}: '{repr(e2)}'\n\n"
                          f"After error '{repr(e)}'")
        return
    # TODO: Check that catalog exists - non-existent catalog will fail

    logger.debug(f"{action_id}: Downloading '{url}'")
    # Download link
    try:
        with open(file_path, 'wb') as output:
            output.write(requests.get(url).content)
    except Exception as e:
        error_status = {
            "status": "FAILED",
            "details": {
                "error": f"Unable to download URL '{url}': {str(e)}"
            }
        }
        try:
            utils.update_action_status(TBL, action_id, error_status)
        except Exception as e2:
            with open("ERROR.log", 'w') as out:
                out.write(f"Error updating status on {action_id}: '{repr(e2)}'\n\n"
                          f"After error '{repr(e)}'")
        return

    # TODO: Use package calls instead of subprocess
    logger.debug(f"{action_id}: Restoring with script")
    try:
        restore_args = [
            "deriva-restore-cli",
            "--oauth2-token",
            token
        ]
        if catalog is not None:
            restore_args.extend([
                "--catalog",
                catalog
            ])
        restore_args.extend([
            "demo.derivacloud.org",
            file_path
        ])
        restore_res = subprocess.run(restore_args, capture_output=True)
        restore_message = restore_res.stderr + restore_res.stdout
    except Exception as e:
        error_status = {
            "status": "FAILED",
            "details": {
                "error": f"Unable to run restore script: {str(e)}"
            }
        }
        try:
            utils.update_action_status(TBL, action_id, error_status)
        except Exception as e2:
            with open("ERROR.log", 'w') as out:
                out.write(f"Error updating status on {action_id}: '{repr(e2)}'\n\n"
                          f"After error '{repr(e)}'")
        return

    # TODO: Check success, fetch ID without needing to parse output text
    try:
        if b"completed successfully" not in restore_message:
            raise ValueError(f"DERIVA restore failed: {restore_message}")
        deriva_link = (restore_message.split(b"Restore of catalog")[-1]
                                      .split(b"completed successfully")[0].strip())
        deriva_id = int(deriva_link.split(b"/")[-1])
        deriva_samples = f"https://demo.derivacloud.org/chaise/recordset/#{deriva_id}/demo:Samples"
    except Exception as e:
        error_status = {
            "status": "FAILED",
            "details": {
                "error": f"Restore script output parsing failed: {str(e)}"
            }
        }
        try:
            utils.update_action_status(TBL, action_id, error_status)
        except Exception as e2:
            with open("ERROR.log", 'w') as out:
                out.write(f"Error updating status on {action_id}: '{repr(e2)}'\n\n"
                          f"After error '{repr(e)}'")
        return

    # Successful restore
    logger.debug(f"{action_id}:Restore complete")
    status = {
        "status": "SUCCEEDED",
        "details": {
            "deriva_id": deriva_id,
            "deriva_samples_link": deriva_samples,
            "message": "DERIVA restore successful"
        }
    }
    try:
        utils.update_action_status(TBL, action_id, status)
    except Exception as e:
        with open("ERROR.log", 'w') as out:
            out.write(f"Error updating status on {action_id}: '{repr(e)}'\n\n"
                      f"After success on ID '{deriva_id}'")
    return


def action_ingest(action_id, url, catalog_id=None, acls=None):
    # Download ingest BDBag
    # Excessive try-except blocks because there's (currently) no process management;
    # if the action fails, it needs to always self-report failure

    logger.debug(f"{action_id}: Deriva ingest process started for {catalog_id or 'new catalog'}")
    # Setup
    try:
        if acls is None:
            acls = CONFIG["DEFAULT_ACLS"]
        data_dir = os.path.join(CONFIG["DATA_DIR"], action_id + "/")
    except Exception as e:
        error_status = {
            "status": "FAILED",
            "details": {
                "error": "Error in action setup: " + str(e)
            }
        }
        # If update fails, last-ditch effort is write to error file for debugging
        try:
            utils.update_action_status(TBL, action_id, error_status)
        except Exception as e2:
            with open("ERROR.log", 'w') as out:
                out.write(f"Error updating status on {action_id}: '{repr(e2)}'\n\n"
                          f"After error '{repr(e)}'")
        return

    # TODO: Check that catalog exists if catalog_id set

    # Download and unarchive link
    logger.debug(f"{action_id}: Downloading '{url}'")
    try:
        dl_res = utils.download_data(None, [url], CONFIG["LOCAL_EP"], data_dir)
        if not dl_res["success"]:
            raise ValueError(str(dl_res))
    except Exception as e:
        error_status = {
            "status": "FAILED",
            "details": {
                "error": f"Unable to download URL '{url}': {str(e)}"
            }
        }
        try:
            utils.update_action_status(TBL, action_id, error_status)
        except Exception as e2:
            with open("ERROR.log", 'w') as out:
                out.write(f"Error updating status on {action_id}: '{repr(e2)}'\n\n"
                          f"After error '{repr(e)}'")
        return

    # Find datapackage JSON file
    schema_file = "File not found"
    logger.debug(f"{action_id}: Determining schema file path")
    try:
        # Get BDBag extract dir (assume exactly one dir)
        bdbag_dir = [dirname for dirname in os.listdir(data_dir)
                     if os.path.isdir(os.path.join(data_dir, dirname))][0]
        # Dir is repeated because of BDBag structure
        bdbag_data = os.path.join(data_dir, bdbag_dir, bdbag_dir, "data")
        # Get schema file (assume exactly one JSON file)
        schema_file = [filename for filename in os.listdir(bdbag_data)
                       if filename.endswith(".json")][0]
        schema_file_path = os.path.join(bdbag_data, schema_file)
    except Exception as e:
        error_status = {
            "status": "FAILED",
            "details": {
                "error": f"Could not process TableSchema file '{schema_file}': {str(e)}"
            }
        }
        try:
            utils.update_action_status(TBL, action_id, error_status)
        except Exception as e2:
            with open("ERROR.log", 'w') as out:
                out.write(f"Error updating status on {action_id}: '{repr(e2)}'\n\n"
                          f"After error '{repr(e)}'")
        return

    # Ingest into Deriva
    logger.debug(f"{action_id}: Ingesting into Deriva")
    try:
        servername = CONFIG["DERIVA_SERVER_NAME"]
        # TODO: Determine schema name from data
        schema_name = CONFIG["DERIVA_SCHEMA_NAME"]

        ingest_res = utils.deriva_ingest(servername, schema_file_path,
                                         catalog_id=catalog_id, acls=acls)
        if not ingest_res["success"]:
            error_status = {
                "status": "FAILED",
                "details": {
                    "error": f"Unable to ingest to DERIVA: {ingest_res.get('error')}"
                }
            }
            utils.update_action_status(TBL, action_id, error_status)
            return
        catalog_id = ingest_res["catalog_id"]
    except Exception as e:
        error_status = {
            "status": "FAILED",
            "details": {
                "error": f"Error ingesting to DERIVA: {str(e)}"
            }
        }
        try:
            utils.update_action_status(TBL, action_id, error_status)
        except Exception as e2:
            with open("ERROR.log", 'w') as out:
                out.write(f"Error updating status on {action_id}: '{repr(e2)}'\n\n"
                          f"After error '{repr(e)}'")
        return

    # Successful ingest
    logger.debug(f"{action_id}: Catalog {catalog_id} populated")
    status = {
        "status": "SUCCEEDED",
        "details": {
            "deriva_id": catalog_id,
            # "number_ingested": insert_count,
            "deriva_link": (f"https://{servername}/chaise/recordset/"
                            f"#{catalog_id}/{schema_name}:Dataset"),
            "message": "DERIVA ingest successful"
        }
    }
    try:
        utils.update_action_status(TBL, action_id, status)
    except Exception as e:
        with open("ERROR.log", 'w') as out:
            out.write(f"Error updating status on {action_id}: '{repr(e)}'\n\n"
                      f"After success on ID '{catalog_id}'")
    return

    '''
    # Old version of Deriva ingest
    # Read and convert TableSchema to ERMrest schema
    # TODO: Will there be exactly on JSON file always? Currently assumed true.
    logger.debug(f"{action_id}: Converting TableSchema to ERMrest")
    schema_file = "File not found"
    try:
        # Get BDBag extract dir (assume exactly one dir)
        bdbag_dir = [dirname for dirname in os.listdir(data_dir)
                     if os.path.isdir(os.path.join(data_dir, dirname))][0]
        bdbag_data = os.path.join(bdbag_dir, "data")
        # Get schema file (assume exactly one JSON file
        schema_file = [filename for filename in os.listdir(bdbag_data)
                       if filename.endswith(".json")][0]
        with open(os.path.join(bdbag_data, schema_file)) as f:
            tableschema = json.load(f)
        ermrest = utils.convert_tableschema(tableschema, CONFIG["DERIVA_SCHEMA_NAME"])
    except Exception as e:
        error_status = {
            "status": "FAILED",
            "details": {
                "error": f"Could not read or convert TableSchema '{schema_file}'"
            }
        }
        try:
            utils.update_action_status(TBL, action_id, error_status)
        except Exception as e2:
            with open("ERROR.log", 'w') as out:
                out.write(f"Error updating status on {action_id}: '{repr(e2)}'\n\n"
                          f"After error '{repr(e)}'")
        return

    # Create new Deriva catalog
    logger.debug(f"{action_id}: Initializing new catalog")
    try:
        catalog_id = utils.create_deriva_catalog(CONFIG["DERIVA_SERVER_NAME"], ermrest, acls)
    except Exception as e:
        error_status = {
            "status": "FAILED",
            "details": {
                "error": f"Unable to create new DERIVA catalog: {str(e)}"
            }
        }
        try:
            utils.update_action_status(TBL, action_id, error_status)
        except Exception as e2:
            with open("ERROR.log", 'w') as out:
                out.write(f"Error updating status on {action_id}: '{repr(e2)}'\n\n"
                          f"After error '{repr(e)}'")
        return

    # Add new entries into catalog
    logger.debug(f"{action_id}: Populating new catalog")
    try:
        for table in tableschema.get("resources", []):
            # Convert TSV to Deriva JSON
            try:
                deriva_data = utils.convert_tabular(os.path.join(bdbag_data, table["path"]))
            except Exception as e:
                missing_msg = "data file not provided in TableSchema"
                error_status = {
                    "status": "FAILED",
                    "details": {
                        "error": ("Unable to convert data file "
                                  f"'{table.get('path', missing_msg)}': {str(e)}")
                    }
                }
                # Error here caught by outer try/except
                utils.update_action_status(TBL, action_id, error_status)
                return
            # Perform insert
            insert_count = 0
            insert_uris = []
            try:
                deriva_result = utils.insert_deriva_data(
                                        CONFIG["DERIVA_SERVER_NAME"], catalog_id,
                                        CONFIG["DERIVA_SCHEMA_NAME"], table["name"], deriva_data)
                if not deriva_result["success"]:
                    raise ValueError(deriva_result.get("error", "insertion unsuccessful"))
                insert_count += deriva_result["num_inserted"]
                insert_uris.append(deriva_result["uri"])
            except Exception as e:
                missing_msg = "table name not provided in TableSchema"
                error_status = {
                    "status": "FAILED",
                    "details": {
                        "error": ("Unable to insert data into table "
                                  f"'{table.get('name', missing_msg)}': {str(e)}")
                    }
                }
                utils.update_action_status(TBL, action_id, error_status)
                return

    except Exception as e:
        error_status = {
            "status": "FAILED",
            "details": {
                "error": f"Unable to populate new DERIVA catalog: {str(e)}"
            }
        }
        try:
            utils.update_action_status(TBL, action_id, error_status)
        except Exception as e2:
            with open("ERROR.log", 'w') as out:
                out.write(f"Error updating status on {action_id}: '{repr(e2)}'\n\n"
                          f"After error '{repr(e)}'")
        return

    # Successful ingest
    logger.debug(f"{action_id}: Catalog populated with {insert_count} entries")
    status = {
        "status": "SUCCEEDED",
        "details": {
            "deriva_id": catalog_id,
            "number_ingested": insert_count,
            "deriva_uris": insert_uris,
            "message": "DERIVA ingest successful"
        }
    }
    try:
        utils.update_action_status(TBL, action_id, status)
    except Exception as e:
        with open("ERROR.log", 'w') as out:
            out.write(f"Error updating status on {action_id}: '{repr(e)}'\n\n"
                      f"After success on ID '{catalog_id}'")
    return
    '''
