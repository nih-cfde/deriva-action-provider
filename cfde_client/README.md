# CFDE Client

This is a client to interact with the Globus Automate Flows for CFDE use cases. Both a Python API and a CLI tool are available.

# Installation

```
git clone https://github.com/fair-research/deriva-action-provider.git
cd deriva-action-provider/cfde_client
pip install -e .
```

# Usage
This tool can ingest any of the following into DERIVA:

1. A directory to be formatted into a BDBag
2. A Git repository to be copied into a BDBag
3. A premade BDBag directory
4. A premade BDBag in an archive file

### Command line
There are two commands available: `run` and `status`. Use them as follows:

- `cfde run DATA-PATH` will ingest the data found at `DATA-PATH` into DERIVA. You can also specify the following options:
    - `--catalog-id=CATALOG_ID` will ingest into an existing catalog instead of creating a new catalog.
    - `--output-dir=OUTPUT_DIR` will copy the data in `DATA-PATH`, if it is a directory, to the location you specify, which must not exist and must not be inside `DATA-PATH`. The resulting BDBag will be named after the output directory. If not specified, the BDBag will be created in-place in `DATA_PATH` if necessary.
    - `--delete-dir` will trigger deletion of the `output-dir` after processing is complete. If you didn't specify `output-dir`, this option has no effect.
    - `--ignore-git` will prevent the client from overwriting `output-dir` and `delete-dir` to handle Git repositories.

- `cfde status` will check the status of a Flow instance. By default, the last run Flow is used, but if you want to check a previous Flow you can provide one or both of the following options:
    - `--flow-id=ID` is the ID of the Flow itself (NOT a specific instance of the Flow).
    - `--flow-instance-id=ID` is the ID of the particular instance/invocation of the Flow you want to check.


### Python API
The `cfdeClient` class, once instantiated, has two primary methods:

- `start_deriva_flow(self, data_path, catalog_id=None, output_dir=None, delete_dir=False, **kwargs)`
- `check_status(self, flow_id=None, flow_instance_id=None, raw=False)`.

The arguments operate in the same fashion as the CLI options, and are documented in detail in the method docstrings.
