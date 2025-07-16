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

def getFCMetadataFields(VO: str) -> dict:
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


def create_empty_metadata_dict(VO: str) -> dict:
    """
    Create an empty metadata dictionary that will be filled later.
    Typically, we list all the required metadata fields and then manually/semi-automatically set them as part of a job.
    :param VO: str (t2k or hk)
    :return: dict with all the required metadata fields but with empty values
    """
    template_dict = getFCMetadataFields(VO)
    return dict.fromkeys(template_dict.keys())

def validate_metadata_fields(VO: str, metadata_dict: dict) -> bool:
    """
    Check the validity of metadata fields in the dictionary.
    Based on the VO, the following return values are:
    - False if :
       - the value in a metadata field is not part of list of possible metadata fields
    - True otherwise
    Warnings are printed if:
    - a metadata field is missing (acceptable in the case where the user is updating some metadata fields)
    - a metadata field is empty (there are fields that could be not applicable based on other metadata values; in that
      case, better to remove them from the dictionary?)
    :param VO: str (t2k or hk)
    :param metadata_dict: dict with the metadata fields to validate
    :return: bool
    """

    template = getFCMetadataFields(VO)

    for key, value in metadata_dict.items():
        if key not in template:
            gLogger.warn(f"Metadata field <{key}> is missing!")
            continue
        if value is None:
            gLogger.warn(f"Metadata field <{key}> is empty!")
        if "values" in template[key] and value not in template[key]["values"]:
            gLogger.error(f"Metadata field <{key}> is invalid: must be within {template[key]["values"]}!")
            return False
    return True
