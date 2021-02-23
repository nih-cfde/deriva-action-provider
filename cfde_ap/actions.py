import logging
from deriva.core import DerivaServer

from cfde_ap import CONFIG
from cfde_ap.auth import get_app_token
from cfde_deriva.registry import Registry
from cfde_deriva.submission import Submission

logger = logging.getLogger(__name__)


DERIVA_INGEST_SUCCESS = 'cfde_registry_dp_status:content-ready'


def deriva_ingest(servername, archive_url, deriva_webauthn_user,
                  dcc_id=None, globus_ep=None, action_id=None):
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
    credential = {
        "bearer-token": get_app_token(CONFIG["DEPENDENT_SCOPES"]["deriva_all"])
    }
    registry = Registry('https', servername, credentials=credential)
    server = DerivaServer('https', servername, credential)

    # the Globus action_id is used as the Submission id, this allows us to track submissions
    # in Deriva back to an action.
    submission_id = action_id
    logger.info(f'Submitting new dataset into Deriva using submission id {submission_id}')

    # pre-flight check like action provider might want to do?
    # this is optional, implicitly happening again in Submission(...)
    registry.validate_dcc_id(dcc_id, deriva_webauthn_user)

    # The Header map protects from submitting our https_token to non-Globus URLs. This MUST
    # match, otherwise the Submission() client will attempt to download the Globus GCS Auth
    # login page instead. r"https://[^/]*[.]data[.]globus[.]org/.*" will match most GCS HTTP pages,
    # but if a custom domain is used this MUST be updated to use that instead.
    https_token = get_app_token(f'https://auth.globus.org/scopes/{globus_ep}/https')
    header_map = {
        CONFIG['ALLOWED_GCS_HTTPS_HOSTS']: {"Authorization": f"Bearer {https_token}"}
    }
    submission = Submission(server, registry, submission_id, dcc_id, archive_url,
                            deriva_webauthn_user, archive_headers_map=header_map)
    submission.ingest()

    md = registry.get_datapackage(submission_id)
    success = md["status"] == DERIVA_INGEST_SUCCESS
    return {
        # status must be a valid automate status ['SUCCEEDED', 'FAILED', 'ACTIVE', 'INACTIVE']
        "status": "SUCCEEDED" if success else "FAILED",
        # Note: The automate flow expects an error to be False if there are no errors.
        # Below ensures in any falsy value from the registry to set error=False so
        # Automate knows there are no errors.
        "error": md.get('diagnostics') or False,
        "message": "DERIVA ingest successful" if success else "",
        "submission_id": submission_id,
        "submission_link": md["review_browse_url"]
    }
