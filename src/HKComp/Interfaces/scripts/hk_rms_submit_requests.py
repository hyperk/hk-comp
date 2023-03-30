#!/bin/env python
"""
Create and put 'ReplicateAndRegister' requests based on a provided yaml file.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from HKComp.Interfaces.Utilities.BaseScript import BaseScript
from HKComp.Interfaces.Utilities.LFNLists import getLFNList
from DIRAC.Core.Utilities.List import breakListIntoChunks

from DIRAC import gLogger, exit as DIRAC_exit


class SubmitRequests(BaseScript):
    '''
    '''
    # defaultSE = "RAL-LCG2-T2K-tape"
    defaultSE = "UKI-LT2-QMUL2-disk,UKI-LT2-IC-HEP-disk,CA-SFU-T21-disk,RAL-LCG2-T2K-tape"
    # defaultSE = "UKI-LT2-QMUL2-disk,UKI-LT2-IC-HEP-disk,CA-SFU-T21-disk"
    switches = [
        ("P:", "pattern=", "Files needs to match pattern", "/", False),
        ("N:", "number=", "Number of files to process", -1, False),
        ('D:', 'subDir=', 'Only register files in the provided directory', "/", False),
        ("S:", "sourceSE=", "Source SE", defaultSE, False),
        ("T:", "targetSE=", "Target SE", None, True),
        ("", "dryrun", "Run in dry-run-mode", False, False),
    ]

    arguments = [
        ("requestName", "Request name", True),
        ("input", "Txt file with LFNs", True)
    ]

    def __init__(self):
        super().__init__()
        self.sizeChunk = 50  # (hardcoded based on discussions with Simon F) # int(self.number)

    def main(self):

        gLogger.info(f"Reading {self.input}")
        file = open(self.input, 'r')
        content = file.read().splitlines()
        self.number = int(self.number) # make sure this is an integer
        lfnList = getLFNList(content, self.subDir, self.pattern, self.number)
        lfnChunks = breakListIntoChunks(lfnList, self.sizeChunk)
        multiRequests = len(lfnChunks) > 1
        gLogger.always(
            "Will create %i request(s) '%s' with 'ReplicateAndRegister' "
            "operation to %s using %s lfns" % (len(lfnChunks), self.requestName, self.targetSE, len(lfnList))
        )
        if self.dryrun:
            gLogger.always("dryrun option: the requests won't be submitted for real")
        gLogger.always(f"Using Source SE {self.sourceSE}")

        from DIRAC.RequestManagementSystem.Client.Request import Request
        from DIRAC.RequestManagementSystem.Client.Operation import Operation
        from DIRAC.RequestManagementSystem.Client.File import File
        from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
        from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

        error = 0
        count = -1
        reqClient = ReqClient()
        fc = FileCatalog()
        requestIDs = []
        for lfnChunk in lfnChunks:
            count += 1
            gLogger.info(f"Round {count}/{len(lfnChunks)-1}")
            metaDatas = fc.getFileMetadata(lfnChunk)
            if not metaDatas["OK"]:
                gLogger.error("unable to read metadata for lfns: %s" % metaDatas["Message"])
                error = -1
                continue
            metaDatas = metaDatas["Value"]
            for failedLFN, reason in metaDatas["Failed"].items():
                gLogger.error("skipping %s: %s" % (failedLFN, reason))
            lfnChunk = set(metaDatas["Successful"])

            if not lfnChunk:
                gLogger.error("LFN list is empty!!!")
                error = -1
                continue

            if len(lfnChunk) > Operation.MAX_FILES:
                gLogger.error("too many LFNs, max number of files per operation is %s" % Operation.MAX_FILES)
                error = -1
                continue

            request = Request()
            request.RequestName = self.requestName if not multiRequests else "%s_%d" % (self.requestName, count)

            replicateAndRegister = Operation()
            replicateAndRegister.Type = "ReplicateAndRegister"
            replicateAndRegister.TargetSE = self.targetSE
            replicateAndRegister.SourceSE = self.sourceSE

            for lfn in lfnChunk:
                metaDict = metaDatas["Successful"][lfn]
                opFile = File()
                opFile.LFN = lfn
                opFile.Size = metaDict["Size"]

                if "Checksum" in metaDict:
                    # # should check checksum type, now assuming Adler32 (metaDict["ChecksumType"] = 'AD'
                    opFile.Checksum = metaDict["Checksum"]
                    opFile.ChecksumType = "ADLER32"
                replicateAndRegister.addFile(opFile)

            request.addOperation(replicateAndRegister)
            if not self.dryrun:
                gLogger.info(f"Submitting request: {request.RequestName}")
                putRequest = reqClient.putRequest(request)
                if not putRequest["OK"]:
                    gLogger.error("unable to put request '%s': %s" % (request.RequestName, putRequest["Message"]))
                    error = -1
                    continue
                requestIDs.append(str(putRequest["Value"]))
            else:
                gLogger.info(f"Not submitting request because of dry-run: {request.RequestName}")
                requestIDs.append(str(count))

            if not multiRequests:
                gLogger.always("Request '%s' has been put to ReqDB for execution." % request.RequestName)

        if multiRequests:
            gLogger.always(
                "%d requests have been put to ReqDB for execution, with name %s_<num>" % (count+1, self.requestName))
        if requestIDs:
            gLogger.always("RequestID(s): %s" % " ".join(requestIDs))
        gLogger.always("You can monitor requests' status using command: 'dirac-rms-request <requestName/ID>'")
        DIRAC_exit(error)


# function defined for setup.cfg
def main():
    script = SubmitRequests()
    script()


# make it able to be run from a shell
if __name__ == "__main__":
    main()
