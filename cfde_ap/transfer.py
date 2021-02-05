import os
import logging
import globus_sdk
import urllib
import datetime

from cfde_ap.auth import get_app_token
from cfde_ap import CONFIG, error

logger = logging.getLogger(__name__)


def move_to_protected_location(url, action_id, dcc_id):
    """Move user submitted datasets to a read-only location, where only the
    Action Provider has write access"""
    transfer_token = get_app_token(CONFIG["DEPENDENT_SCOPES"]["transfer"])
    auth = globus_sdk.AccessTokenAuthorizer(transfer_token)
    tc = globus_sdk.TransferClient(authorizer=auth)

    purl = urllib.parse.urlparse(url)
    # DCCs *should* always be of the pattern "cfde_registry_dcc:kidsfirst"
    _, dcc_name = dcc_id.rsplit(":", 1)
    # New dataset name should look like:
    # /CFDE/public/kidsfirst/1612387394-05167ed4-6666-11eb-9dcc-784f43874631.tgz
    dcc_dir = os.path.join(CONFIG["LONG_TERM_STORAGE"], dcc_name)

    _, old_ext = os.path.splitext(purl.path)
    new_filename = f"{datetime.datetime.now().isoformat()}-{action_id}{old_ext}"
    new_dataset_path = os.path.join(dcc_dir, new_filename)
    logger.debug(f'Renaming dataset "{purl.path}" to "{new_dataset_path}"')
    try:
        tc.operation_rename(CONFIG["GCS_ENDPOINT"], purl.path, new_dataset_path)
    except globus_sdk.exc.TransferAPIError as tapie:
        if tapie.code == "EndpointError":
            raise error.DeveloperError(f"Failed to rename '{purl.path}' to '{new_dataset_path}', "
                                       f"Check {dcc_dir} exists!") from tapie
        raise
    url = urllib.parse.urlunparse((purl.scheme, purl.netloc, new_dataset_path, "", "", ""))
    logger.debug(f"Successfully updated dataset URL to {url}")
    return url
