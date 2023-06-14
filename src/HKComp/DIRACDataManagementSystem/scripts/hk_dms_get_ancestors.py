#!/usr/bin/env python
"""
A utility to display the ancestors of an LFN
"""

from HKComp.DIRACCore.Utilities.BaseScript import BaseScript

from DIRAC import gLogger
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

# dirac requires that we set this
__RCSID__ = '$Id$'


class GetAncestors(BaseScript):
    """
        Determine the analysis that have been done for a given RID (based on directories that exist)

        Positional arg is a full LFN
    """
    switches = [
        ('u:', 'depth=', 'Max number of generations to trace', 1, False),
    ]

    arguments = [
        ("lfn", "LFN to get ancestors", True),  # currently this only can find the ancestors of one file
    ]

    def main(self):
        fcc = FileCatalogClient()
        # signature: getFileAncestors( self, lfns, depths, timeout = 120 ):
        result = fcc.getFileAncestors(self.lfn, int(self.depth))
        worked = result['Value']['Successful']
        for lfn, ancestors in worked.items():
            gLogger.always('{}:\n{}'.format(lfn, '\n'.join(
                ['    > {}'.format(a) for a in ancestors] or ['    > None'])))


# function defined for setup.cfg
def main():
    script = GetAncestors()
    script()


# make it able to be run from a shell
if __name__ == "__main__":
    main()
