import logging
import globus_sdk
from flask import request
from deriva.core.utils.globus_auth_utils import GlobusAuthUtil
from cfde_deriva.submission import WebauthnUser, WebauthnAttribute

from cfde_ap.config import CONFIG

logger = logging.getLogger(__name__)


def get_app_token(scope):
    cc_app = globus_sdk.ConfidentialAppAuthClient(
        CONFIG["GLOBUS_CC_APP"],
        CONFIG["GLOBUS_SECRET"],
    )
    access_token = globus_sdk.ClientCredentialsAuthorizer(
        scopes=scope,  # Deriva scope
        confidential_client=cc_app
    ).access_token
    logger.debug(f"Retrieved dependent token for scope '{scope}'")
    return access_token


def get_webauthn_user():
    gau = GlobusAuthUtil(
        client_id=CONFIG["GLOBUS_CC_APP"],
        client_secret=CONFIG["GLOBUS_SECRET"],
    )
    new_user_info = gau.get_userinfo_for_token(request.auth.bearer_token)
    return WebauthnUser(
            new_user_info['client']['id'],
            new_user_info['client']['display_name'],
            new_user_info['client'].get('full_name'),
            new_user_info['client'].get('email'),
            [
                WebauthnAttribute(attr['id'], attr.get('display_name', 'unknown'))
                for attr in new_user_info['attributes']
            ]
    )
