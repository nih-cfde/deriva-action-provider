import json
import os

import click

from cfde_client import CfdeClient


DEFAULT_STATE_FILE = os.path.expanduser("~/.cfde_client.json")


@click.group()
def cli():
    """Client to interact with the DERIVA Action Provider and associated Flows."""
    pass


@cli.command()
@click.argument("data-path", nargs=1, type=click.Path(exists=True))
@click.option("--catalog", default=None, show_default=True)
@click.option("--schema", default=None, show_default=True)
@click.option("--output-dir", default=None, show_default=True, type=click.Path(exists=False))
@click.option("--delete-dir/--keep-dir", is_flag=True, default=False, show_default=True)
@click.option("--ignore-git/--handle-git", is_flag=True, default=False, show_default=True)
# TODO: Debug "hidden" missing parameter
@click.option("--server", default=None)  # , hidden=True)
@click.option("--bag-kwargs-file", type=click.Path(exists=True), default=None)  # , hidden=True)
@click.option("--client-state-file", type=click.Path(exists=True), default=None)  # , hidden=True)
def run(data_path, catalog, schema, output_dir, delete_dir, ignore_git,
        server, bag_kwargs_file, client_state_file):
    """Start the Globus Automate Flow to ingest CFDE data into DERIVA."""
    if bag_kwargs_file:
        with open(bag_kwargs_file) as f:
            bag_kwargs = json.load(f)
    else:
        bag_kwargs = {}
    if not client_state_file:
        client_state_file = DEFAULT_STATE_FILE

    try:
        cfde = CfdeClient()
        start_res = cfde.start_deriva_flow(data_path, catalog_id=catalog, schema=schema,
                                           output_dir=output_dir, delete_dir=delete_dir,
                                           handle_git_repos=(not ignore_git),
                                           server=server, **bag_kwargs)
    except Exception as e:
        print("Error while starting Flow: {}".format(str(e)))
        return
    else:
        if not start_res["success"]:
            print("Error during Flow startup: {}".format(start_res["error"]))
        else:
            with open(client_state_file, 'w') as out:
                json.dump(start_res, out)
            print(start_res["message"])


@cli.command()
@click.option("--flow-id", default=None, show_default=True)
@click.option("--flow-instance-id", default=None, show_default=True)
@click.option("--client-state-file", type=click.Path(exists=True), default=None)  # , hidden=True)
def status(flow_id=None, flow_instance_id=None, client_state_file=None):
    """Check the status of a Flow."""
    if not flow_id or not flow_instance_id:
        if not client_state_file:
            client_state_file = DEFAULT_STATE_FILE
        try:
            with open(client_state_file) as f:
                client_state = json.load(f)
            flow_id = flow_id or client_state.get("flow_id")
            flow_instance_id = flow_instance_id or client_state.get("flow_instance_id")
            if not flow_id or not flow_instance_id:
                raise ValueError("flow_id or flow_instance_id not found")
        except (FileNotFoundError, ValueError):
            print("Flow not started and flow-id or flow-instance-id not specified")
            return
    try:
        cfde = CfdeClient()
        status_res = cfde.check_status(flow_id, flow_instance_id, raw=True)
    except Exception as e:
        print("Error checking status for Flow '{}': {}".format(flow_id, str(e)))
        return
    else:
        print(status_res["clean_status"])
