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
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

from DIRAC import gLogger, exit as DIRAC_exit

from collections import OrderedDict

class SubmitSplitRequests(BaseScript):
    '''
    '''
    # defaultSE = "RAL-LCG2-T2K-tape"
    # defaultSE = "UKI-LT2-QMUL2-disk,UKI-LT2-IC-HEP-disk,CA-SFU-T21-disk"
    defaultSE = "CA-SFU-T21-disk,RAL-LCG2-T2K-tape,UKI-LT2-IC-HEP-disk,UKI-LT2-QMUL2-disk"

    targetSEDict = OrderedDict()
    targetSEDict["IN2P3-CC-XRD-disk"] = ["cata", "anal"]
    targetSEDict["None"] = ["log", "logf"]
    targetSEDict["IN2P3-CC-XRD-tape"] = ["t2k.org"] # basically everything else should go to tape

    switches = [
        ("P:", "pattern=", "Files needs to match pattern", "/", False),
        ("N:", "number=", "Number of files to process", -1, False),
        ("n:", "start=", "Start processing at given number", 0, False),
        ('D:', 'subDir=', 'Only register files in the provided directory', "/", False),
        ("S:", "sourceSE=", "Source SE", defaultSE, False),
        # ("T:", "targetSE=", "Target SE", None, True),
        ("", "dryrun", "Run in dry-run-mode", False, False),
    ]

    arguments = [
        ("requestName", "Request name", True),
        ("input", "Txt file with LFNs", True)
    ]

    def __init__(self):
        super().__init__()
        self.sizeChunk = 50  # size of a request

    def main(self):

        gLogger.info(f"Reading {self.input}")
        file = open(self.input, 'r')
        content = file.read().splitlines()
        self.number = int(self.number) # make sure this is an integer
        self.start = int(self.start) # make sure this is an integer
        lfnList = getLFNList(content[self.start:], self.subDir, self.pattern, self.number)
        lfnChunks = breakListIntoChunks(lfnList, self.sizeChunk)
        # gLogger.always(
        #     "Will create %i request(s) '%s' with 'ReplicateAndRegister' "
        #     "operation to %s using %s lfns" % (len(lfnChunks), self.requestName, self.targetSE, len(lfnList))
        # )
        if self.dryrun:
            gLogger.always("dryrun option: the requests won't be submitted for real")
        self.list_sourceSE = self.sourceSE.split(",")
        gLogger.always(f"Using Source SE {self.list_sourceSE}")

        error = 0
        count = -1
        self.requestIDs = []
        fc = FileCatalog()
        counter_no_common_se = 0

        requests_counter = dict() # counter for incrementing the number of requests
        submit_requests = dict() # contains a list of files for target SE; we add files to each list based on preferences; if longer than 50, submit the request and set the list to []
        for lfnChunk in lfnChunks:
            count += 1
            gLogger.info(f"Round {count}/{len(lfnChunks)-1}")
            if not lfnChunk:
                gLogger.error("LFN list is empty!!!")
                error = -1
                continue
            replicas = fc.getReplicas(lfnChunk)
            if not replicas["OK"]:
                gLogger.error("Error while getting the replicas")
                continue
            print(f'Length found replicas: {len(replicas["Value"]["Successful"])}')
            for a_file, subdict in replicas["Value"]["Successful"].items():
                common_SE = set(self.list_sourceSE).intersection(subdict.keys())
                if len(common_SE) == 0:
                    print(f"no common SE between {self.list_sourceSE} and {subdict.keys()} for {a_file}; skipping!")
                    counter_no_common_se+=1
                    continue
                # item = "/t2k.org/nd280/production006/A/fpp/verify/v11r21/ND280/00008000_00008999/anal/oa_nd_spl_00008995-0011_f7fxycgrroep_anal_000_v11r21-wg-bugaboo-bsdv01_2.root"
                # subdict = { 'IN2P3-CC-XRD-tape': lfn, 'IN2P3-CC-XRD-disk': lfn }
                # print("->"+item)
                added_to_submit = False
                file_already_in_se = False
                for targetSE, list_types in self.targetSEDict.items(): # targetSE = "IN2P3-CC-XRD-disk"; list_types = ["cata", "something"]
                    found = False
                    for a_type in list_types:
                        # print(f'testing {"/" + a_type + "/"} in {item}')
                        if "/" + a_type + "/" in a_file:
                            found = True
                            break
                    if found:
                        # print("Found!")
                        # print(f'{targetSE} vs {subdict.keys()}')
                        if targetSE in subdict.keys(): # the file is already at the targetSE; skip it
                            gLogger.info(f"{a_file} already on {targetSE}" )
                            file_already_in_se = True
                            break
                        if targetSE not in submit_requests.keys():
                            submit_requests.update({targetSE: []})
                            requests_counter.update({targetSE: 0})

                        submit_requests[targetSE].append(a_file)
                        added_to_submit = True
                        if len(submit_requests[targetSE]) >= self.sizeChunk:
                            gLogger.info(f"Length for {targetSE} reached 50")
                            a_requestName = self.requestName+"_"+targetSE+"_"+str(requests_counter[targetSE])
                            if not self.dryrun:
                                gLogger.always(f"Submitting request: {a_requestName} -> {len(submit_requests[targetSE])} files to {targetSE}")
                                self.submitRequest(a_requestName, self.sourceSE, targetSE, submit_requests[targetSE])
                            else:
                                gLogger.info(f"Not submitting request because of dry-run: {a_requestName}")
                                self.requestIDs.append(str(count))
                            submit_requests[targetSE] = [] # cleaning this list
                            requests_counter[targetSE] += 1
                        break
                if not added_to_submit and not file_already_in_se:
                    gLogger.warn(f"Failed to add file {a_file} to submission")

        for targetSE, list_files in submit_requests.items():
            if len(submit_requests[targetSE]) == 0:
                continue
            a_requestName = self.requestName + "_" + targetSE + "_" + str(requests_counter[targetSE])
            if not self.dryrun:
                gLogger.always(
                    f"Submitting request: {a_requestName} -> {len(submit_requests[targetSE])} files to {targetSE}")
                self.submitRequest(a_requestName, self.sourceSE, targetSE, submit_requests[targetSE])
            else:
                gLogger.info(f"Not submitting request because of dry-run: {a_requestName}")
                self.requestIDs.append(str(count))
            submit_requests[targetSE] = []  # cleaning this list
            requests_counter[targetSE] += 1
            # if len(lfnChunk) > Operation.MAX_FILES:
            #     gLogger.error("too many LFNs, max number of files per operation is %s" % Operation.MAX_FILES)
            #     error = -1
            #     continue



            # if not multiRequests:
            #     gLogger.always("Request '%s' has been put to ReqDB for execution." % request.RequestName)

        # if multiRequests:
        #     gLogger.always(
        #         "%d requests have been put to ReqDB for execution, with name %s_<num>" % (count+1, requestName))
        if self.requestIDs:
            gLogger.always("RequestID(s): %s" % " ".join(self.requestIDs))
            gLogger.always("You can monitor requests' status using command: 'dirac-rms-request <requestName/ID>'")
        else:
            gLogger.always("No request submitted")
        gLogger.always(f"Number of files not-submitted because of no common SE: {counter_no_common_se}")
        DIRAC_exit(error)

    def submitRequest(self, requestName, sourceSE, targetSE, list_files):
        if targetSE == "None":
            return

        fc = FileCatalog()
        reqClient = ReqClient()
        metaDatas = fc.getFileMetadata(list_files)
        if not metaDatas["OK"]:
            gLogger.error("unable to read metadata for lfns: %s" % metaDatas["Message"])
            error = -1
            return
        metaDatas = metaDatas["Value"]
        for failedLFN, reason in metaDatas["Failed"].items():
            gLogger.error("skipping %s: %s" % (failedLFN, reason))
        list_files = list(metaDatas["Successful"])


        request = Request()
        request.RequestName = requestName

        replicateAndRegister = Operation()
        replicateAndRegister.Type = "ReplicateAndRegister"
        replicateAndRegister.TargetSE = targetSE
        replicateAndRegister.SourceSE = sourceSE

        for lfn in list_files:
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
        gLogger.info(f"Submitting request: {request.RequestName}")
        put_request = reqClient.putRequest(request)
        if not put_request["OK"]:
            gLogger.error("unable to put request '%s': %s" % (requestName, put_request["Message"]))
            error = -1
            return
        self.requestIDs.append(str(put_request["Value"]))


# function defined for setup.cfg
def main():
    script = SubmitSplitRequests()
    script()


# make it able to be run from a shell
if __name__ == "__main__":
    main()
