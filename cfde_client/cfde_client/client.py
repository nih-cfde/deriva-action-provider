import os

from bdbag import bdbag_api
# from fair_research_login import NativeClient, LoadError, ScopesMismatch
import globus_automate_client
import globus_sdk
import requests


# class CfdeClient(NativeClient):
class CfdeClient():
    """The CfdeClient enables easily using the CFDE tools to ingest data."""
    client_id = "417301b1-5101-456a-8a27-423e71a2ae26"
    required_scopes = []
    transfer_flow_id = "b1e13f5e-dad6-4524-8c3f-fb39e752a266"
    http_flow_id = "f172de09-b75b-4b83-9b97-90877b42c774"
    archive_format = "tgz"
    fair_re_dir = "/public/CFDE/metadata/"
    fair_re_url = "https://317ec.36fe.dn.glob.us"

    def __init__(self):
        self.flow_client = globus_automate_client.create_flows_client(self.client_id)
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
            put_res = requests.put()
            flow_id = self.http_flow_id
            flow_input = {
                "data_url": "{}{}".format(self.fair_re_url, fair_re_path)
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

    def check_status(self, flow_id=None, flow_instance_id=None):
        """Check the status of a Flow. By default, check the status of the last
        Flow run with this instantiation of the client.

        Arguments:
            flow_id (str): The ID of the Flow run. Default: The last run Flow ID.
            flow_instance_id (str): The ID of the Flow to check.
                    Default: The last Flow instance run with this client.
        """
        if not flow_id:
            flow_id = self.last_flow_run.get("flow_id")
        if not flow_instance_id:
            flow_instance_id = self.last_flow_run.get("flow_instance_id")
        if not flow_id or not flow_instance_id:
            raise ValueError("Flow not started and IDs not specified.")

        # Get Flow scope and status
        flow_scope = self.flow_client.get_flow(flow_id)["globus_auth_scope"]
        flow_status = self.flow_client.flow_action_status(flow_id, flow_scope,
                                                          flow_instance_id).data

        return {
            "success": True,
            "status": flow_status,
            # TODO: Prettify clean_status
            "clean_status": flow_status
        }
