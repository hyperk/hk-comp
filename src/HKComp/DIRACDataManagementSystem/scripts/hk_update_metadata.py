#!/usr/bin/env python
'''
'''

import json

from HKComp.DIRACCore.Utilities.BaseScript import BaseScript
from HKComp.HKTools.Utilities.FCMetadataFields import getFCMetadataFields

# dirac requires that we set this
__RCSID__ = '$Id$'


class UpdateMetadata(BaseScript):
    """
    This script checks all the metadata present in the file catalog, add the missing ones, and reports the incorrect ones.
    The list of expected metadata is defined in HKComp.HKTools.Utilities.FCMetadataFields as dictionaries.
    One should REALLY use these dictionaries (and this script after adding a metadata) in order to make sure we know what we put there.
    Requires to write the VO to update via parameter (t2k or hk)
    """
    switches = [
        ("", "apply", "Actually apply the changes", False, False),
    ]
    arguments = [
        ('VO', 'VO to update (t2k or hk)', True)
    ]

    def __init__(self):
        super().__init__()

    def main(self):

        from DIRAC.Interfaces.API.Dirac import Dirac
        from DIRAC import S_OK, S_ERROR, gLogger, exit as DIRAC_Exit

        self.dirac = Dirac()

        self.desired_metadata_fields = getFCMetadataFields(self.VO)

        from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

        fcc = FileCatalogClient()

        res = fcc.getMetadataFields()

        if not res["OK"]:
            print("Failed to extract metadata from FC")
            exit(1)

        existing_metadata_fields = res["Value"]["FileMetaFields"]
        gLogger.info(f"Existing fields:\n{json.dumps(existing_metadata_fields, sort_keys=True, indent=4)}")

        if not self.apply:
            gLogger.always("Running in dry-mode: use --apply for doing the update!")

        for key, value in self.desired_metadata_fields.items():
            if key in existing_metadata_fields.keys():
                gLogger.info(f"Metadata {key} already present: checking type...")
                if existing_metadata_fields[key] != value["type"]:
                    gLogger.warn(
                        f"Metadata {key} doesn't have the right type: {existing_metadata_fields[key]} != {value['type']}")
                else:
                    gLogger.info(f"Expected type: {value['type']}")
                continue
            gLogger.info(f"Adding the metadata {key}->{value['type']} to FC")
            if not self.apply:
                gLogger.always("Not running the command")
            else:
                gLogger.info("Running the command")
                # res = fcc.addMetadataField(key, value['type'], '-f') # TODO uncomment me
            if res["OK"]:
                gLogger.info(f"Done!")
            else:
                gLogger.info(f"Error while adding the field: {res['Value']}")

# make it able to be run from a shell
if __name__ == "__main__":
    script = UpdateMetadata()
    script()
