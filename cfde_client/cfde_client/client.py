import os
import shutil

from bdbag import bdbag_api
from datapackage import Package
from fair_research_login import NativeClient
import git
import globus_automate_client
import globus_sdk
import requests

from cfde_client import CONFIG


def ts_validate(data_path, schema=None):
    """Validate a given TableSchema using the Datapackage package.

    Arguments:
        data_path (str): Path to the TableSchema JSON or BDBag directory
                or BDBag archive to validate.
        schema (str): The schema to validate against. If not provided,
                the data is only validated against the defined TableSchema.
                Default None.

    Returns:
        dict: The validation results.
            is_valid (bool): Is the TableSchema valid?
            raw_errors (list): The raw Exceptions generated from any validation errors.
            error (str): A formatted error message about any validation errors.
    """
    # If data_path is BDBag archive, unarchive to temp dir
    try:
        data_path = bdbag_api.extract_bag(data_path, temp=True)
    # data_path is not archive
    except RuntimeError:
        pass
    # If data_path is dir (incl. if was unarchived), find JSON desc
    if os.path.isdir(data_path):
        # If 'data' dir present, search there instead
        if "data" in os.listdir(data_path):
            data_path = os.path.join(data_path, "data")
        # Find .json file
        desc_file_list = [filename for filename in os.listdir(data_path)
                          if filename.endswith(".json")]
        if len(desc_file_list) < 1:
            return {
                "is_valid": False,
                "raw_errors": [FileNotFoundError("No TableSchema JSON file found.")],
                "error": "No TableSchema JSON file found."
            }
        elif len(desc_file_list) > 1:
            return {
                "is_valid": False,
                "raw_errors": [RuntimeError("Multiple JSON files found in directory.")],
                "error": "Multiple JSON files found in directory."
            }
        else:
            data_path = os.path.join(data_path, desc_file_list[0])
    # data_path should/must be file now (JSON desc)
    if not os.path.isfile(data_path):
        return {
            "is_valid": False,
            "raw_errors": [ValueError("Path '{}' does not refer to a file".format(data_path))],
            "error": "Path '{}' does not refer to a file".format(data_path)
        }

    # Read into Package, return error on failure
    try:
        pkg = Package(data_path)
    except Exception as e:
        return {
            "is_valid": False,
            "raw_errors": [e],
            "error": str(e)
        }

    # TODO: Pull schema from existing catalog, validate against that too
    if schema:
        print("Warning: Currently unable to validate data against existing schema '{}'."
              .format(schema))

    # Actually check and return package validity based on Package validation
    return {
        "is_valid": pkg.valid,
        "raw_errors": pkg.errors,
        "error": "\n".join([str(err) for err in pkg.errors])
    }


class CfdeClient():
    """The CfdeClient enables easily using the CFDE tools to ingest data."""
    client_id = "417301b1-5101-456a-8a27-423e71a2ae26"
    app_name = "CfdeClient"
    archive_format = "tgz"

    def __init__(self, **kwargs):
        """Create a CfdeClient.

        Keyword Arguments:
            no_browser (bool): Do not automatically open the browser for the Globus Auth URL.
                    Display the URL instead and let the user navigate to that location manually.
                    **Default**: ``False``.
            refresh_tokens (bool): Use Globus Refresh Tokens to extend login time.
                    **Default**: ``True``.
            force (bool): Force a login flow, even if loaded tokens are valid.
                    Same effect as ``clear_old_tokens``. If one of these is ``True``, the effect
                    triggers. **Default**: ``False``.
        """
        self.__native_client = NativeClient(client_id=self.client_id, app_name=self.app_name)
        self.__native_client.login(requested_scopes=CONFIG["ALL_SCOPES"],
                                   no_browser=kwargs.get("no_browser", False),
                                   refresh_tokens=kwargs.get("refresh_tokens", True),
                                   force=kwargs.get("force", False))
        tokens = self.__native_client.load_tokens_by_scope()
        flows_token_map = {scope: token["access_token"] for scope, token in tokens.items()}
        automate_authorizer = self.__native_client.get_authorizer(
                                    tokens[globus_automate_client.flows_client.MANAGE_FLOWS_SCOPE])
        self.__https_authorizer = self.__native_client.get_authorizer(tokens[CONFIG["HTTPS_SCOPE"]])
        self.flow_client = globus_automate_client.FlowsClient(
                                                    flows_token_map, self.client_id, "flows_client",
                                                    app_name=self.app_name,
                                                    base_url="https://flows.automate.globus.org",
                                                    authorizer=automate_authorizer)
        self.local_endpoint = globus_sdk.LocalGlobusConnectPersonal().endpoint_id
        self.last_flow_run = {}

    def start_deriva_flow(self, data_path, catalog_id=None, schema=None, server=None,
                          output_dir=None, delete_dir=False, handle_git_repos=True, **kwargs):
        """Start the Globus Automate Flow to ingest CFDE data into DERIVA.

        Arguments:
            data_path (str): The path to the data to ingest into DERIVA. The path can be:
                    1) A directory to be formatted into a BDBag
                    2) A Git repository to be copied into a BDBag
                    3) A premade BDBag directory
                    4) A premade BDBag in an archive file
            catalog_id (int or str): The ID of the DERIVA catalog to ingest into.
                    Default None, to create a new catalog.
            schema (str): The named schema or schema file link to validate data against.
                    Default None, to only validate against the declared TableSchema.
            server (str): The DERIVA server to ingest to.
                    Default None, to use the Action Provider-set default.
            output_dir (str): The path to create an output directory in. The resulting
                    BDBag archive will be named after this directory.
                    If not set, the directory will be turned into a BDBag in-place.
                    For Git repositories, this is automatically set, but can be overridden.
                    If data_path is a file, this has no effect.
                    This dir MUST NOT be in the `data_path` directory or any subdirectories.
                    Default None.
            delete_dir (bool): Should the output_dir be deleted after submission?
                    Has no effect if output_dir is not specified.
                    For Git repositories, this is always True.
                    Default False.
            handle_git_repos (bool): Should Git repositories be detected and handled?
                    When this is False, Git repositories are handled as simple directories
                    instead of Git repositories.
                    Default True.

        Keyword arguments are passed directly to the ``make_bag()`` function of the
        BDBag API (see https://github.com/fair-research/bdbag for details).
        """
        data_path = os.path.abspath(data_path)
        if not os.path.exists(data_path):
            raise FileNotFoundError("Path '{}' does not exist".format(data_path))

        if catalog_id in CONFIG["KNOWN_CATALOGS"]:
            if schema:
                raise ValueError("You may not specify a schema ('{}') when ingesting to "
                                 "a named catalog ('{}'). Retry without specifying "
                                 "a schema.".format(schema, catalog_id))
            schema = CONFIG["KNOWN_CATALOGS"][catalog_id]

        if handle_git_repos:
            # If Git repo, set output_dir appropriately
            try:
                repo = git.Repo(data_path, search_parent_directories=True)
            # Not Git repo
            except git.InvalidGitRepositoryError:
                pass
            # Path not found, turn into standard FileNotFoundError
            except git.NoSuchPathError:
                raise FileNotFoundError("Path '{}' does not exist".format(data_path))
            # Is Git repo
            else:
                # Needs to not have slash at end - is known Git repo already, slash
                # interferes with os.path.basename/dirname
                if data_path.endswith("/"):
                    data_path = data_path[:-1]
                # Set output_dir to new dir named with HEAD commit hash
                new_dir_name = "{}_{}".format(os.path.basename(data_path), str(repo.head.commit))
                output_dir = os.path.join(os.path.dirname(data_path), new_dir_name)
                # Delete temp dir after archival
                delete_dir = True

        # If dir and not already BDBag, make BDBag
        if os.path.isdir(data_path) and not bdbag_api.is_bag(data_path):
            # If output_dir specified, copy data to output dir first
            if output_dir:
                output_dir = os.path.abspath(output_dir)
                # If shutil.copytree is called when the destination dir is inside the source dir
                # by more than one layer, it will recurse infinitely.
                # (e.g. /source => /source/dir/dest)
                # Exactly one layer is technically okay (e.g. /source => /source/dest),
                # but it's easier to forbid all parent/child dir cases.
                # Check for this error condition by determining if output_dir is a child
                # of data_path.
                if os.path.commonpath([data_path]) == os.path.commonpath([data_path, output_dir]):
                    raise ValueError("The output_dir ('{}') must not be in data_path ('{}')"
                                     .format(output_dir, data_path))
                try:
                    shutil.copytree(data_path, output_dir)
                except FileExistsError:
                    raise FileExistsError(("The output directory must not exist. "
                                           "Delete '{}' to submit.\nYou can set delete_dir=True "
                                           "to avoid this issue in the future.").format(output_dir))
                # Process new dir instead of old path
                data_path = output_dir
            # If output_dir not specified, never delete data dir
            else:
                delete_dir = False
            # Make bag
            bdbag_api.make_bag(data_path, **kwargs)
            if not bdbag_api.is_bag(data_path):
                raise ValueError("Failed to create BDBag from {}".format(data_path))

        # If dir (must be BDBag at this point), archive
        if os.path.isdir(data_path):
            new_data_path = bdbag_api.archive_bag(data_path, CONFIG["ARCHIVE_FORMAT"])
            # If requested (e.g. Git repo copied dir), delete data dir
            if delete_dir:
                shutil.rmtree(data_path)
            # Overwrite data_path - don't care about dir for uploading
            data_path = new_data_path

        # Validate TableSchema in BDBag
        validation_res = ts_validate(data_path, schema=schema)
        if not validation_res["is_valid"]:
            return {
                "success": False,
                "error": ("TableSchema invalid due to the following errors: \n{}\n"
                          .format(validation_res["error"]))
            }

        # Now BDBag is archived file
        # Set path on destination (FAIR RE EP)
        dest_path = "{}{}".format(CONFIG["EP_DIR"], os.path.basename(data_path))

        # Create Flow input
        # If a local EP exists, use Transfer Flow
        if self.local_endpoint:
            flow_id = CONFIG["TRANSFER_FLOW"]
            flow_input = {
                "source_endpoint_id": self.local_endpoint,
                "source_path": data_path,
                "fair_re_path": dest_path,
                "is_directory": False,
                "restore": False
            }
            if catalog_id:
                flow_input["catalog_id"] = str(catalog_id)
        # Otherwise, we must PUT the BDBag on the FAIR RE EP
        else:
            headers = {}
            self.__https_authorizer.set_authorization_header(headers)
            data_url = "{}{}".format(CONFIG["EP_URL"], dest_path)

            with open(data_path, 'rb') as bag_file:
                bag_data = bag_file.read()

            put_res = requests.put(data_url, data=bag_data, headers=headers)

            # Regenerate headers on 401
            if put_res.status_code == 401:
                self.__https_authorizer.handle_missing_authorization()
                self.__https_authorizer.set_authorization_header(headers)
                put_res = requests.put(data_url, data=bag_data, headers=headers)

            # Error message on failed PUT or any unexpected response
            if put_res.status_code >= 300:
                return {
                    "success": False,
                    "error": ("Could not upload BDBag to server (error {}):\n{}"
                              .format(put_res.status_code, put_res.content))
                }

            flow_id = CONFIG["HTTP_FLOW"]
            flow_input = {
                "data_url": data_url
            }
            if catalog_id:
                flow_input["catalog_id"] = str(catalog_id)

        # Get Flow scope
        flow_def = self.flow_client.get_flow(flow_id)
        flow_scope = flow_def["globus_auth_scope"]
        # Start Flow
        flow_res = self.flow_client.run_flow(flow_id, flow_scope, flow_input)
        self.last_flow_run = {
            "flow_id": flow_id,
            "flow_instance_id": flow_res["action_id"]
        }

        return {
            "success": True,
            "message": ("Started DERIVA ingest Flow\nFlow ID: {}\nFlow Instance ID: {}"
                        .format(flow_id, flow_res["action_id"])),
            "flow_id": flow_id,
            "flow_instance_id": flow_res["action_id"]
        }

    def check_status(self, flow_id=None, flow_instance_id=None, raw=False):
        """Check the status of a Flow. By default, check the status of the last
        Flow run with this instantiation of the client.

        Arguments:
            flow_id (str): The ID of the Flow run. Default: The last run Flow ID.
            flow_instance_id (str): The ID of the Flow to check.
                    Default: The last Flow instance run with this client.
            raw (bool): Should the status results be returned?
                    Default: False, to print the results instead.
        """
        if not flow_id:
            flow_id = self.last_flow_run.get("flow_id")
        if not flow_instance_id:
            flow_instance_id = self.last_flow_run.get("flow_instance_id")
        if not flow_id or not flow_instance_id:
            raise ValueError("Flow not started and IDs not specified.")

        # Get Flow scope and status
        flow_def = self.flow_client.get_flow(flow_id)
        flow_status = self.flow_client.flow_action_status(flow_id, flow_def["globus_auth_scope"],
                                                          flow_instance_id).data

        # Create user-friendly version of status message
        STATE_MSGS = CONFIG["STATE_MSGS"]
        clean_status = "\nStatus of {} (instance {})\n".format(flow_def["title"], flow_instance_id)
        # Flow overall status
        clean_status += "This Flow {}.\n".format(STATE_MSGS[flow_status["status"]])
        # "Details"
        if flow_status["details"].get("details"):
            if flow_status["details"]["details"].get("state_name"):
                clean_status += ("Current Flow Step: {}"
                                 .format(flow_status["details"]["details"]["state_name"]))
            if flow_status["details"]["details"].get("cause"):
                clean_status += "Error: {}\n".format(flow_status["details"]["details"]["cause"])
        # TransferResult
        if flow_status["details"].get("output", {}).get("TransferResult"):
            transfer_status = flow_status["details"]["output"]["TransferResult"]["status"]
            transfer_result = flow_status["details"]["output"]["TransferResult"]["details"]
            clean_status += "The Globus Transfer {}.\n".format(STATE_MSGS[transfer_status])
            if transfer_result["status"] == "SUCCEEDED":
                clean_status += ("\t{} bytes were transferred.\n"
                                 .format(transfer_result["bytes_transferred"]))
            elif transfer_result["status"] == "FAILED":
                clean_status += ("\tError: {}\n"
                                 .format(transfer_result["fatal_error"]
                                         or transfer_result["nice_status"]
                                         or transfer_result["canceled_by_admin_message"]
                                         or ("Unknown error on task '{}'"
                                             .format(transfer_result["task_id"]))))
        # DerivaResult
        if flow_status["details"].get("output", {}).get("DerivaResult"):
            deriva_status = flow_status["details"]["output"]["DerivaResult"]["status"]
            deriva_result = flow_status["details"]["output"]["DerivaResult"]["details"]
            clean_status += "The DERIVA ingest {}.\n".format(STATE_MSGS[deriva_status])
            if deriva_result.get("error"):
                clean_status += "\tError: {}\n".format(deriva_result["error"])
            if deriva_result.get("message"):
                clean_status += "\t{}\n".format(deriva_result["message"])
            if deriva_result.get("deriva_link"):
                clean_status += "\tCatalog ID: {}\n".format(deriva_result["deriva_id"])
                clean_status += "\tLink to catalog: {}\n".format(deriva_result["deriva_link"])

        # Return or print status
        if raw:
            return {
                "success": True,
                "status": flow_status,
                "clean_status": clean_status
            }
        else:
            print(clean_status)
