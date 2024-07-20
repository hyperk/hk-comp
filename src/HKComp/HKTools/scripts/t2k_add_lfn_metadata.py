#!/usr/bin/env python
"""
A utility to display the ancestors of an LFN
"""
import fnmatch
from pathlib import Path

from HKComp.DIRACCore.Utilities.BaseScript import BaseScript
from HKComp.HKTools.Utilities.FCMetadataFields import create_empty_metadata_dict, validate_metadata_fields
from HKComp.HKTools.Utilities.run_periods import get_t2k_run_period

from DIRAC import gLogger, exit as dirac_exit
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

# dirac requires that we set this
__RCSID__ = '$Id$'


class UpdateMetadata(BaseScript):
    """
        Positional arg is a full LFN
    """
    switches = [
        ('u:', 'depth=', 'Max number of generations to trace', 1, False),
    ]

    arguments = [
        ("path", "Path to files", True),
    ]

    ### List of ND280 run ids associated with a T2K run
    # Using https://t2k.org/nd280/datacomp/production/production007/rdp/7E_13.28 for reference
    runs_ids = {
        2: "6462,7-7754,16",
        3: "8360,23-8753,18",
        4: "8995,5-9796,4",
        5: "10252,10-10521,13",
        6: "10932,8-11687,127",
        7: "12080,12-12556,2",
        8: "12716,1-13737,2",
        9: "13846,9-14417,2"
    }

    def main(self):
        fcc = FileCatalogClient()
        result = fcc.listDirectory(self.path)
        if result["OK"]:
            list_files = list(result["Value"]["Successful"][self.path]["Files"].keys())

        for file_path in list_files:

            metadata_dict = create_empty_metadata_dict("t2k")
            if self.path.startswith("/t2k.org/nd280/raw"):
                # gLogger.always("Reading raw MIDAS files")
                metadata_dict.update({
                    "type": "data",
                    "data_level": "raw"
                })
                file_name = Path(file_path).name
                if not fnmatch.fnmatch(file_name, "*_*_*.daq.mid.gz"):
                    gLogger.warn(f"Issue with filename {file_name}: not matching *_*_*.daq.mid.gz")
                    continue
                detector, run_number, subrun_number = file_name.replace(".daq.mid.gz", "").split('_')
                print(file_name, " -> ", detector, run_number, subrun_number)
                run_details = get_t2k_run_period(int(run_number),int(subrun_number))
                run_number = run_details.run_number
                run_letter = run_details.run_letter
                metadata_dict.update({
                    "detector": detector,
                    "run_id": f"{run_number}{run_letter}"
                })
                print(metadata_dict)
                validate_metadata_fields("t2k", metadata_dict)
                exit(0)

            else:
                gLogger.fatal("Not-supported data files type")
                dirac_exit(1)

        # fcc.setMetadata()
# function defined for setup.cfg
def main():
    script = UpdateMetadata()
    script()


# make it able to be run from a shell
if __name__ == "__main__":
    main()
