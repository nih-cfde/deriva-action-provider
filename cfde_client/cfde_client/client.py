import os

from bdbag import bdbag_api
from fair_research_login import NativeClient
import globus_automate_client
import globus_sdk
import requests


STATE_MSGS = {
    "ACTIVE": "is still in progress",
    "INACTIVE": "has stalled, and may need help to resume",
    "SUCCEEDED": "has completed successfully",
    "FAILED": "has failed"
}


class CfdeClient():
    """The CfdeClient enables easily using the CFDE tools to ingest data."""
    client_id = "417301b1-5101-456a-8a27-423e71a2ae26"
    app_name = "CfdeClient"
    https_scope = "https://auth.globus.org/scopes/0e57d793-f1ac-4eeb-a30f-643b082d68ec/https"
    all_scopes = (list(globus_automate_client.flows_client.ALL_FLOW_SCOPES) + [https_scope])
    transfer_flow_id = "b1e13f5e-dad6-4524-8c3f-fb39e752a266"
    http_flow_id = "056c26d7-3bf4-4022-8b4d-029875b5e8c0"
    archive_format = "tgz"
    fair_re_dir = "/public/CFDE/metadata/"
    fair_re_url = "https://317ec.36fe.dn.glob.us"

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
        self.__native_client.login(requested_scopes=self.all_scopes,
                                   no_browser=kwargs.get("no_browser", False),
                                   refresh_tokens=kwargs.get("refresh_tokens", True),
                                   force=kwargs.get("force", False))
        tokens = self.__native_client.load_tokens_by_scope()
        flows_token_map = {scope: token["access_token"] for scope, token in tokens.items()}
        automate_authorizer = self.__native_client.get_authorizer(
                                    tokens[globus_automate_client.flows_client.MANAGE_FLOWS_SCOPE])
        self.__https_authorizer = self.__native_client.get_authorizer(tokens[self.https_scope])
        self.flow_client = globus_automate_client.FlowsClient(
                                                    flows_token_map, self.client_id, "flows_client",
                                                    app_name=self.app_name,
                                                    base_url="https://flows.automate.globus.org",
                                                    authorizer=automate_authorizer)
        self.local_endpoint = globus_sdk.LocalGlobusConnectPersonal().endpoint_id
        self.last_flow_run = {}

    def start_deriva_flow(self, data_path, **kwargs):
        """Start the Globus Automate Flow to ingest CFDE data into DERIVA.

        Arguments:
            data_path (str): The path to the data to ingest into DERIVA. The path can be:
                    1) A directory to be made into a BDBag
                    2) A premade BDBag directory
                    3) A premade BDBag in an archive file

        Keyword arguments are passed directly to the ``make_bag()`` function of the
        BDBag API (see https://github.com/fair-research/bdbag for details).
        """
        data_path = os.path.abspath(data_path)
        # If dir and not already BDBag, make BDBag
        if os.path.isdir(data_path) and not bdbag_api.is_bag(data_path):
            bdbag_api.make_bag(data_path, **kwargs)
            if not bdbag_api.is_bag(data_path):
                raise ValueError("Failed to create BDBag from {}".format(data_path))
        # If dir (must be BDBag at this point), archive
        if os.path.isdir(data_path):
            # Overwrite data_path - don't care about dir for uploading
            data_path = bdbag_api.archive_bag(data_path, self.archive_format)

        # Now BDBag is archived file
        # Set path on destination (FAIR RE EP)
        fair_re_path = "/public/CFDE/metadata/{}".format(os.path.basename(data_path))

        # Create Flow input
        # If a local EP exists, use Transfer Flow
        if self.local_endpoint:
            flow_id = self.transfer_flow_id
            flow_input = {
                "source_endpoint_id": self.local_endpoint,
                "source_path": data_path,
                "fair_re_path": fair_re_path,
                "is_directory": False,
                "restore": False
            }
        # Otherwise, we must PUT the BDBag on the FAIR RE EP
        else:
            # TODO
            headers = {}
            self.__https_authorizer.set_authorization_header(headers)
            data_url = "{}{}".format(self.fair_re_url, fair_re_path)

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

            flow_id = self.http_flow_id
            flow_input = {
                "data_url": data_url
            }

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
        clean_status = "\nStatus of {} (instance {})\n".format(flow_def["title"], flow_instance_id)
        # Flow overall status
        clean_status += "This Flow {}.\n".format(STATE_MSGS[flow_status["status"]])
        # "Details"
        if flow_status["details"].get("details"):
            clean_status += "{}\n".format(flow_status["details"]["details"]
                                          .get("cause", flow_status["details"]["details"]))
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
