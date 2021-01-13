import logging
import globus_sdk
from flask import request
from deriva.core.utils.globus_auth_utils import GlobusAuthUtil
from cfde_deriva.submission import WebauthnUser, WebauthnAttribute

from cfde_ap.config import CONFIG

logger = logging.getLogger(__name__)


def get_dependent_token(scope):
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


def get_deriva_token():
    # TODO: When decision is made about user auth vs. conf client auth, implement.
    #       Currently using personal refresh token for scope.
    #       See keys.py for last-generated date.

    return globus_sdk.RefreshTokenAuthorizer(
                        refresh_token=CONFIG["TEMP_REFRESH_TOKEN"],
                        auth_client=globus_sdk.NativeAppAuthClient(CONFIG["GLOBUS_NATIVE_APP"])
           ).access_token


def _generate_new_deriva_token():
    # Generate new Refresh Token to be used in get_deriva_token()
    native_client = globus_sdk.NativeAppAuthClient(CONFIG["GLOBUS_NATIVE_APP"])
    native_flow = native_client.oauth2_start_flow(
                                    requested_scopes=("https://auth.globus.org/scopes/"
                                                      "app.nih-cfde.org/deriva_all"),
                                    refresh_tokens=True)
    code = input(f"Auth at '{native_flow.get_authorize_url()}' and paste code:\n")
    tokens = native_flow.exchange_code_for_tokens(code)
    return tokens["refresh_token"]