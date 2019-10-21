import json
import os

import click

from cfde_client import CfdeClient


@click.group()
def cli():
    """Client to interact with the DERIVA Action Provider and associated Flows."""
    pass


@cli.command()
@click.argument("data-path", nargs=1, type=click.Path(exists=True))
@click.option("--bag-kwargs-file", type=click.Path(exists=True))
@click.option("--client-state-file", type=click.Path(exists=True))
def run(data_path, bag_kwargs_file=None, client_state_file=None):
    """Start the Globus Automate Flow to ingest CFDE data into DERIVA."""
    if bag_kwargs_file:
        with open(bag_kwargs_file) as f:
            bag_kwargs = json.load(f)
    else:
        bag_kwargs = {}
    if not client_state_file:
        client_state_file = os.path.expanduser("~/.cfde_client.json")

    try:
        cfde = CfdeClient()
        start_res = cfde.start_deriva_flow(data_path, **bag_kwargs)
    except Exception as e:
        print("Error while starting Flow: {}".format(str(e)))
        return
    else:
        with open(client_state_file, 'w') as out:
            json.dump(start_res, out)
        print(start_res["message"])


@cli.command()
@click.option("--flow-id")
@click.option("--flow-instance-id")
@click.option("--client-state-file", type=click.Path(exists=True))
def status(flow_id=None, flow_instance_id=None, client_state_file=None):
    """Check the status of a Flow."""
    if not flow_id or not flow_instance_id:
        if not client_state_file:
            client_state_file = os.path.expanduser("~/.cfde_client.json")
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
        status_res = cfde.check_status(flow_id, flow_instance_id)
    except Exception as e:
        print("Error checking status for Flow '{}': {}".format(flow_id, str(e)))
        return
    else:
        print(status_res["clean_status"])
