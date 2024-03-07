from DIRAC import S_OK, S_ERROR, gLogger, exit as DIRAC_Exit


# For T2K and HK, uses: https://docs.google.com/spreadsheets/d/1zqqIeeIU0vHvs82Hxfxq5q98zk4FTzuuhVMT5f7joL0/edit?usp=sharing
t2k_metadata_fields = {
    "type": {
        "type": "VARCHAR(24)",
        "values": ["mc", "data", "param"],
    },
    "detector": {
        "type": "VARCHAR(24)",
        "values": ["nd280", "ingrid", "hat", "sfgd", "dog"],
    },
    "nd280_version": {
        "type": "VARCHAR(24)",
    },
    "prod_version": {
        "type": "VARCHAR(24)",
    },
    "package_config_version": {
        "type": "VARCHAR(24)",
    },
    "data_level": {
        "type": "VARCHAR(24)",
        "values": ["raw", "preproc", "calib", "reco", "cata", "log"],
    },
    "run_id": {
        "type": "INT UNSIGNED",
    },
    "geometry_id": {
        "type": "VARCHAR(24)",
    },
}
hk_metadata_fields = {
    "type": {
        "type": "VARCHAR(24)",
        "values": ["mc", "data", "param"],
    },
    "detector": {
        "type": "VARCHAR(24)",
        "values": ["nd280", "iwcd", "hk"],
    },
    "nd280_version": {
        "type": "VARCHAR(24)",
    },
    "prod_version": {
        "type": "VARCHAR(24)",
    },
    "package_config_version": {
        "type": "VARCHAR(24)",
    },
    "data_level": {
        "type": "VARCHAR(24)",
        "values": ["raw", "preproc", "calib", "reco", "cata", "log"],
    },
    "run_id": {
        "type": "INT UNSIGNED",
    },
    "geometry_id": {
        "type": "VARCHAR(24)",
    },
}

def getFCMetadataFields(VO: str):
    """
    This returns the proper dictionary of metadata based on the VO
    :param VO: VO to use (t2k, hk, ...)
    :return: dict
    """

    if not VO in ["t2k", "hk"]:
        gLogger.fatal(f"Cannot handle VO {VO} different from t2k or hk")
        DIRAC_Exit(1)

    # Only handles T2K (for now)
    if VO == "hk":
        return hk_metadata_fields
    elif VO == "t2k":
        return t2k_metadata_fields