# For T2K and HK, uses: https://docs.google.com/spreadsheets/d/1zqqIeeIU0vHvs82Hxfxq5q98zk4FTzuuhVMT5f7joL0/edit?usp=sharing

t2k_metadata_fields = {
    "type": {
        "type": "VARCHAR",
        "values": ["mc", "data", "param"],
    },
    "nd280_version": {
        "type": "VARCHAR",
    },
    "prod_version": {
        "type": "VARCHAR",
    },
    "package_config_version": {
        "type": "VARCHAR",
    },
    "data_level": {
        "type": "VARCHAR",
        "values": ["raw", "preproc", "calib", "reco", "cata"],
    },
    "run_id": {
        "type": "UINT",
    },
    "geometry_id": {
        "type": "VARCHAR",
    },
}