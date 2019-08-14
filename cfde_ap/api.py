from datetime import datetime, timedelta, timezone
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
from openapi_core.wrappers.flask import FlaskOpenAPIResponse, FlaskOpenAPIRequest
import requests

from cfde_ap import CONFIG
from . import error as err, utils


# Flask setup
app = Flask(__name__)
app.config.from_mapping(**CONFIG)
app.url_map.strict_slashes = False

# Logging setup
logger = logging.getLogger("cfde_ap")
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
TOKEN_CHECKER = TokenChecker(CONFIG["GLOBUS_ID"], CONFIG["GLOBUS_SECRET"],
                             [CONFIG["GLOBUS_SCOPE"]], CONFIG["GLOBUS_AUD"])


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
        "runnable_by": ["all_authenticated_users"],
        # "administered_by": [],
        # "admin_contact": "",
        "synchronous": False,
        "log_supported": False,
        # "maximum_deadline": "",
        # "input_schema": {},
        # "event_types": [],  # Event-type providers only
    }
    if not request.auth.check_authorization(resp["visible_to"],
                                            allow_all_authenticated_users=True):
        raise err.NotAuthorized("You cannot view this Action Provider.")
    return jsonify(resp)


@app.route(ROOT+"run", methods=["POST"])
def run():
    req = request.get_json(force=True)
    # If request_id has been submitted before, return status instead of starting new
    try:
        status = utils.read_action_by_request(TBL, req["request_id"])
    except err.NotFound:
        # Create new action

        # TODO: Validate request body, add schema to / return
        body = req["body"]
        if not body:
            req["body"] = {}

        # TODO: Accurately estimate completion time
        estimated_completion = datetime.now(tz=timezone.utc) + timedelta(days=1)

        # The Automate spec says this should be an ISO8601 duration,
        # but that is apparently out of date or changing.
        # Number of seconds is permissable.
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
    url = action_data["url"]
    logger.info(f"{action_id}: Starting Deriva ingest")
    # Spawn new process
    # TODO: Process management
    #       Currently assuming process manages itself
    driver = multiprocessing.Process(target=restore_deriva, args=(action_id, url), name=action_id)
    driver.start()
    return


def cancel_action(action_id):
    # This action doesn't implement cancellation,
    # which is valid according to the Automate spec.
    # This is a stub in case cancellation is implemented later.
    return


#######################################
# Asynchronous action
#######################################

def restore_deriva(action_id, url):
    # TODO: Real auth
    token = CONFIG["TEMP_TOKEN"]

    # Download backup zip file
    # TODO: Determine file type
    #       Use original file name (Content-Disposition)
    #       Make filename unique if collision
    #       Set better base path than local dir

    # Excessive try-except blocks because there's (currently) no process management;
    # if the action fails, it needs to always self-report failure

    # Setup
    try:
        base_path = os.getcwd()
        file_path = os.path.join(base_path, "cfde-backup.zip")
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
    try:
        restore_res = subprocess.run(["deriva-restore-cli", "--oauth2-token", token,
                                      "demo.derivacloud.org", file_path], capture_output=True)
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
        if not b"completed successfully" in restore_message:
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
    return
