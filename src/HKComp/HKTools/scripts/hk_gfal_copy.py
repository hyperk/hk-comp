#!/bin/env python
"""
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from HKComp.DIRACCore.Utilities.BaseScript import BaseScript

from DIRAC import gLogger
import gfal2


class GFALTestingScript(BaseScript):
    '''
    '''
    switches = [
        # ("T:", "targetSE=", "Target SE", None, True),
    ]

    arguments = [
        # ("requestName", "Request name", True),
    ]

    def __init__(self):
        super().__init__()

    def main(self):
        gLogger.info(f"Doing things")
        context = gfal2.creat_context()
        a = context.stat("root://x509up_u525827271@ccxroot.in2p3.fr:1097///xrootd/in2p3.fr/disk/t2k.org/t2k.org/nd280/raw/nd280/test/test_file_sept6_8.txt")
        a = context.lstat("root://x509up_u525827271@ccxroot.in2p3.fr:1097///xrootd/in2p3.fr/disk/t2k.org/t2k.org/nd280/raw/nd280/test/test_file_sept6_8.txt")
        print(type(a))
        print(a)




# function defined for setup.cfg
def main():
    script = GFALTestingScript()
    script()


# make it able to be run from a shell
if __name__ == "__main__":
    main()