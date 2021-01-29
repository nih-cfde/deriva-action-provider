import fair_research_login
import globus_sdk
from cfde_deriva.registry import Registry


DERIVA_SCOPE = 'https://auth.globus.org/scopes/app.nih-cfde.org/deriva_all'
TRANSFER_SCOPE = 'urn:globus:auth:scope:transfer.api.globus.org:all'
CFDE_NATIVE_APP = '417301b1-5101-456a-8a27-423e71a2ae26'

nc = fair_research_login.NativeClient(client_id=CFDE_NATIVE_APP)
tokens = nc.login(requested_scopes=[DERIVA_SCOPE, TRANSFER_SCOPE])


def get_groups(servername):
    credentials = {
        "bearer-token": nc.load_tokens_by_scope()[DERIVA_SCOPE]['access_token']
    }
    registry = Registry('https', servername, credentials=credentials)
    groups = registry.get_groups_by_dcc_role(role_id='cfde_registry_grp_role:submitter')
    from pprint import pprint
    pprint(groups)


def get_acls(globus_endpoint):
    tc_auth = nc.get_authorizers_by_scope()[TRANSFER_SCOPE]
    print(tc_auth)
    tc = globus_sdk.TransferClient(authorizer=tc_auth)

    response = tc.endpoint_acl_list(globus_endpoint)
    from pprint import pprint
    pprint(response.data)



if __name__ == '__main__':
    # get_groups('app-dev.nih-cfde.org')
    get_acls('36530efa-a1e3-45dc-a6e7-9560a8e9ac49')
