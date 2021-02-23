import click
import collections
import copy
import datetime
import fair_research_login
import globus_automate_client
import globus_sdk
import json
import os
import requests
from cfde_ap.auth import get_app_token
from cfde_ap import CONFIG as CFDE_CONFIG
from cfde_deriva.registry import Registry
from flow import full_submission_flow_def

native_app_id = "417301b1-5101-456a-8a27-423e71a2ae26"  # Premade native app ID
deriva_aps = {
    "dev": "https://ap-dev.nih-cfde.org/",
    "staging": "https://ap-staging.nih-cfde.org/",
    "prod": "https://ap.nih-cfde.org/"
}

client_config_filename = os.path.join(os.path.dirname(__file__), "cfde_client_config.json")
old_flows_filename = os.path.join(os.path.dirname(__file__), "old_flows.txt")
DERIVA_SCOPE = 'https://auth.globus.org/scopes/app.nih-cfde.org/deriva_all'
TRANSFER_SCOPE = 'urn:globus:auth:scope:transfer.api.globus.org:all'
CFDE_NATIVE_APP = '417301b1-5101-456a-8a27-423e71a2ae26'
nc = fair_research_login.NativeClient(client_id=CFDE_NATIVE_APP)
nc.login(requested_scopes=[DERIVA_SCOPE, TRANSFER_SCOPE])
transfer_token = get_app_token(CFDE_CONFIG["DEPENDENT_SCOPES"]['transfer'])
auth = globus_sdk.AccessTokenAuthorizer(transfer_token)
transfer_client = globus_sdk.TransferClient(authorizer=auth)


def load_client_config():
    with open(client_config_filename) as f:
        return json.JSONDecoder(object_pairs_hook=collections.OrderedDict).decode(f.read())


@click.group()
def cli():
    pass


@cli.command(help="Deploy the latest flow definition in flow.py")
@click.option("--service", default="dev")  # , hidden=True)
def deploy_flow(service):
    """Deploy the latest flow, and track the id in cfde_client_config. Old flows
    are not automatically deleted, and are tracked in old_flows_filename"""
    flows_client = globus_automate_client.create_flows_client(native_app_id)
    serv = deriva_aps[service]
    client_config = load_client_config()
    full_submission_flow_def["definition"]["States"]["DerivaIngest"]["ActionUrl"] = serv
    deriva_server = CFDE_CONFIG['DEFAULT_SERVER_NAME']

    admins = get_groups(deriva_server, 'admin')
    submitters = get_groups(deriva_server, 'submitter')
    reviewers = get_groups(deriva_server, 'reviewer')
    approvers = get_groups(deriva_server, 'approver')

    writers = [admins, submitters]
    flow_runnable_urns = set_acls_from_groups(writers, "rw")

    readers = [reviewers, approvers]
    set_acls_from_groups(readers, "r")

    flow_runnable_urns = list(set(flow_runnable_urns))
    full_flow_deploy_res = flows_client.deploy_flow(
        flow_definition=full_submission_flow_def["definition"],
        title=full_submission_flow_def["title"],
        description=full_submission_flow_def["description"],
        visible_to=flow_runnable_urns,
        runnable_by=flow_runnable_urns,
    )
    click.secho(f"[{service}] Flow Deployed: {full_flow_deploy_res['id']}", fg="green")

    with open(old_flows_filename, mode='a') as f:
        f.write(f"Replaced {datetime.datetime.now().isoformat()} -- "
                f"{full_flow_deploy_res['id']}\n")

    client_config["FLOWS"][service]["flow_id"] = full_flow_deploy_res["id"]

    with open(client_config_filename, "w+") as f:
        f.write(json.dumps(client_config, indent=4))
    click.secho(f"Client Config Updated '{client_config_filename}'", fg="green")

    # Here we could check the current version of the CLI, and update the minimum requirement
    "https://pypi.org/pypi/cfde-submit/json"


@cli.command(help="Update the existing flow ID for a service with new definition changes")
@click.option("--service", default="dev")  # , hidden=True)
def update_flow(service):
    flows_client = globus_automate_client.create_flows_client(native_app_id)
    serv = deriva_aps[service]
    client_config = load_client_config()
    flow_id = client_config["FLOWS"][service]["flow_id"]
    full_submission_flow_def["definition"]["States"]["DerivaIngest"]["ActionUrl"] = serv


    flows_client.update_flow(
        flow_id,
        full_submission_flow_def["definition"],
    )
    click.secho(f'Updated flow {flow_id}', fg='green')
    click.secho('NOTE: No provisioning took place. If any dependent scopes were added to the flow, '
                'this may result in any future flow instances failing', fg='yellow')

@cli.command(help="Deploy client-config for public usage")
def deploy_client_config():
    cli = fair_research_login.NativeClient(client_id=native_app_id)
    scope = "https://auth.globus.org/scopes/d1e360d2-3b83-4039-bd82-f38f5bf2c394/https"
    cli.login(requested_scopes=scope)
    headers = {"Authorization": f"Bearer {cli.load_tokens_by_scope()[scope]['access_token']}"}
    url = ("https://g-5cf005.aa98d.08cc.data.globus.org/submission_dynamic_config/"
           "cfde_client_config.json")
    put_res = requests.put(url, json=load_client_config(), headers=headers)
    put_res.raise_for_status()
    click.secho(f"Client Config Deployed: '{url}'", fg="green")


@cli.command(help="Ensure client config is the latest shown here")
def check_config():
    if get_remote_config() == load_client_config():
        click.secho('Remote Config is up to date with local config', fg='green')
    else:
        click.secho(f'Remote config differs from local config: {client_config_filename}', fg='red')


@cli.command(help="Download remote config")
def download_config():
    if input(f'Overwrite {client_config_filename}? (y/n)> ') == 'y':
        with open(client_config_filename, 'w+') as f:
            f.write(json.dumps(get_remote_config(), indent=4))
        click.secho(f'Saved to {client_config_filename}', fg='green')


def get_remote_config():
    cli = fair_research_login.NativeClient(client_id=native_app_id)
    scope = "https://auth.globus.org/scopes/d1e360d2-3b83-4039-bd82-f38f5bf2c394/https"
    cli.login(requested_scopes=scope)
    headers = {"Authorization": f"Bearer {cli.load_tokens_by_scope()[scope]['access_token']}",
               "X-Requested-With": "XMLHttpRequest"}
    url = ("https://g-5cf005.aa98d.08cc.data.globus.org/submission_dynamic_config/"
           "cfde_client_config.json")
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def get_groups(servername, role):
    credentials = {
        "bearer-token": nc.load_tokens_by_scope()[DERIVA_SCOPE]['access_token']
    }
    registry = Registry('https', servername, credentials=credentials)
    groups = registry.get_groups_by_dcc_role(role_id=f'cfde_registry_grp_role:{role}')
    return groups


def create_dir(path):
    try:
        transfer_client.operation_ls(CFDE_CONFIG["GCS_ENDPOINT"], path=path)
    except globus_sdk.exc.TransferAPIError as tapie:
        if tapie.code != "ClientError.NotFound":
            raise
        transfer_client.operation_mkdir(CFDE_CONFIG["GCS_ENDPOINT"], path=path)


def create_acl(path, group, permissions):
    endpoint = CFDE_CONFIG['GCS_ENDPOINT']
    existing_rules = transfer_client.endpoint_acl_list(endpoint)

    rule = {'DATA_TYPE': 'access',
            'path': path,
            'permissions': permissions,
            'principal': group,
            'principal_type': 'group',
            'role_id': None,
            'role_type': None}

    for existing_rule in existing_rules:
        existing_rule.pop("id")
        existing_rule.pop("permissions")
        rule_copy = copy.deepcopy(rule)
        rule_copy.pop("permissions")
        if existing_rule == rule_copy:
            return

    return transfer_client.add_endpoint_acl_rule(endpoint, rule)


def set_acls_from_groups(group_lists, cfde_data_permissions):
    urns = list()
    for group_list in group_lists:
        for dcc_dict in group_list:
            dcc = dcc_dict['dcc']
            dcc_name = dcc.split(':')[-1]
            for group in dcc_dict['groups']:
                gid = group['id']
                urn = f"urn:globus:groups:id:{gid}"
                urns.append(urn)
                group_dir = os.path.join(CFDE_CONFIG["LONG_TERM_STORAGE"], dcc_name) + "/"
                create_dir(group_dir)
                create_acl(group_dir, gid, "r")
                create_acl("/CFDE/data/", gid, cfde_data_permissions)
    return urns


if __name__ == "__main__":
    cli()
