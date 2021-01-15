import globus_automate_client
import collections
import requests
import datetime
import fair_research_login
import json
import time
import pprint
from deriva.core import DerivaServer
import os

import click
from flow import full_submission_flow_def

native_app_id = "417301b1-5101-456a-8a27-423e71a2ae26"  # Premade native app ID
deriva_aps = {
    "dev": "https://ap-dev.nih-cfde.org/",
    "staging": "https://ap-staging.nih-cfde.org/",
    "prod": "https://ap.nih-cfde.org/"
}
client_config_filename = os.path.join(os.path.dirname(__file__),
                                      "cfde_client_config.json")
old_flows_filename = os.path.join(os.path.dirname(__file__), "old_flows.txt")


def load_client_config():
    with open(client_config_filename) as f:
        return json.JSONDecoder(object_pairs_hook=collections.OrderedDict).decode(f.read())

@click.group()
def cli():
    pass


@cli.command(help="Deploy the latest flow definition in flow.py")
@click.option("--service", default="dev")  # , hidden=True)
def flow(service):
    """Deploy the latest flow, and track the id in cfde_client_config. Old flows
    are not automatically deleted, and are tracked in old_flows_filename"""
    flows_client = globus_automate_client.create_flows_client(native_app_id)
    serv = deriva_aps[service]
    client_config = load_client_config()
    full_submission_flow_def["definition"]["States"]["DerivaIngest"]["ActionUrl"] = serv
    full_flow_deploy_res = flows_client.deploy_flow(
        flow_definition=full_submission_flow_def["definition"],
        title=full_submission_flow_def["title"],
        description=full_submission_flow_def["description"],
        visible_to=full_submission_flow_def["visible_to"],
        runnable_by=full_submission_flow_def["runnable_by"],
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


@cli.command(help="Deploy client-config for public usage")
def client_config():
    """De"""
    cli = fair_research_login.NativeClient(client_id=native_app_id)
    scope = "https://auth.globus.org/scopes/d1e360d2-3b83-4039-bd82-f38f5bf2c394/https"
    cli.login(requested_scopes=scope)
    headers = {"Authorization": f"Bearer {cli.load_tokens_by_scope()[scope]['access_token']}"}
    url = ("https://g-5cf005.aa98d.08cc.data.globus.org/submission_dynamic_config/"
           "cfde_client_config.json")
    put_res = requests.put(url, json=load_client_config(), headers=headers)
    put_res.raise_for_status()
    click.secho(f"Client Config Deployed: '{url}'", fg="green")


if __name__ == "__main__":
    cli()
