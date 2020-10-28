DEFAULT_ACLS = {
    # The UUIDs here are Globus Groups, in the form "https://auth.globus.org/<UUID>"
    # For more information on these groups, visit
    # "https://app.globus.org/groups/<UUID>"
    "owner": [
        # Demo Admin
        "https://auth.globus.org/5a773142-e2ed-11e8-a017-0e8017bdda58",
        # Demo Creator
        "https://auth.globus.org/bc286232-a82c-11e9-8157-0ed6cb1f08e0"
    ],
    "insert": [
        # Demo Curator
        "https://auth.globus.org/a5cfa412-e2ed-11e8-a768-0e368f3075e8",
        # Demo Writer
        "https://auth.globus.org/caa11064-e2ed-11e8-9d6d-0a7c1eab007a"
    ],
    "update": [
        # Demo Curator
        "https://auth.globus.org/a5cfa412-e2ed-11e8-a768-0e368f3075e8",
        # Demo Writer
        "https://auth.globus.org/caa11064-e2ed-11e8-9d6d-0a7c1eab007a"
    ],
    "delete": [
        # Demo Curator
        "https://auth.globus.org/a5cfa412-e2ed-11e8-a768-0e368f3075e8",
        # Demo Writer
        "https://auth.globus.org/caa11064-e2ed-11e8-9d6d-0a7c1eab007a"
    ],
    "select": [
        # Demo Reader
        "https://auth.globus.org/b9100ea4-e2ed-11e8-8b39-0e368f3075e8",
        # ISRD Staff
        "https://auth.globus.org/176baec4-ed26-11e5-8e88-22000ab4b42b",
        # ISRD Testers
        "https://auth.globus.org/9d596ac6-22b9-11e6-b519-22000aef184d"
    ],
    "enumerate": [
        # All (?)
        "*"
    ]
}
