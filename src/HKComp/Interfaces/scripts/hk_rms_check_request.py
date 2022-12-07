#!/bin/env python
"""
Check the status of the set of RMS requests
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from HKComp.Interfaces.Utilities.BaseScript import BaseScript

from DIRAC import gLogger, exit as DIRAC_exit


class CheckRequests(BaseScript):
    '''
    Check the status of the set of RMS requests and display some information.
    Can give one request or a range of requests ID e.g. "000003-000007"
    In the case of a failed request, it stores the non-replicated files into a file that could be re-submitted
    '''
    switches = [
        ('O:', 'output=', 'Suffix of the files to store results in', None, False),
    ]

    arguments = [
        ("requestName", "Request name", True),
    ]

    def __init__(self):
        super().__init__()
        self.requestsList = list()

    def main(self):

        gLogger.info(f"Checking request(s) {self.requestName}")
        if "-" in self.requestName:
            startID, endID = self.requestName.split("-")
            self.requestsList = [i for i in range(int(startID), int(endID)+1)]
        else:
            self.requestsList = [self.requestName]

        gLogger.always(f"Getting status of {len(self.requestsList)} request(s): {self.requestsList}")

        from DIRAC.RequestManagementSystem.Client.Request import Request
        from DIRAC.RequestManagementSystem.Client.Operation import Operation
        from DIRAC.RequestManagementSystem.Client.File import File
        from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
        from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

        from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient

        list_failed_replication = []
        list_failed_replication_requests = []
        list_failed_registration = []
        numberRegisterOps = 0
        numberReplicationOps = 0
        numberFilesToRegister = 0
        numberFilesToReplicate = 0
        numberRegisterOpsInProgress = 0
        numberReplicationOpsInProgress = 0

        rc = ReqClient() # # create client

        for requestID in self.requestsList:

            operations = rc.peekRequest( requestID )['Value']._getJSONData()['Operations']
            for item in operations:
                if item.Type == "RegisterReplica":
                    numberRegisterOps += 1
                    for file in item._getJSONData()['Files']:
                        numberFilesToRegister += 1
                elif item.Type == "ReplicateAndRegister":
                    for file in item._getJSONData()['Files']:
                        numberFilesToReplicate += 1
                    numberReplicationOps += 1
                if item.Status == "Failed":
                    list_failed_replication_requests.append(item.RequestID)
                    for file in item._getJSONData()['Files']:
                        if file.Status == "Failed":
                            if item.Type == "RegisterReplica":
                                list_failed_registration.append(file.LFN)
                            elif item.Type == "ReplicateAndRegister":
                                list_failed_replication.append(file.LFN)
                            else:
                                gLogger.always(f"Unsupported type: {item.Type}")
                elif item.Status in ["Queued", "Waiting", "Scheduled", "Assigned"]:
                    if item.Type == "RegisterReplica":
                        numberRegisterOpsInProgress += 1
                    elif item.Type == "ReplicateAndRegister":
                        numberReplicationOpsInProgress += 1


        if (numberReplicationOps != numberRegisterOps):
            gLogger.always(f"Some RegisterReplica ops are not started?: {numberRegisterOps} != {numberReplicationOps}")
        else:
            gLogger.always("All RegisterReplica started!")
        gLogger.always(f"Number of failed ReplicateAndRegister: {len(list_failed_replication)}/{numberFilesToReplicate}")
        gLogger.always(f"Number of failed RegisterReplica: {len(list_failed_registration)}/{numberFilesToRegister}")
        gLogger.always(f"Number of ReplicateAndRegister ops in progress: {numberReplicationOpsInProgress}/{numberReplicationOps}")
        gLogger.always(f"Number of RegisterReplica ops in progress: {numberRegisterOpsInProgress}/{numberRegisterOps}")

        gLogger.always(list_failed_replication_requests)

        if self.output is None:
            return
        with open("not_replicated_"+self.output, "w") as text_file:
            for element in list_failed_replication:
                text_file.write(element + "\n")

        with open("not_registered_"+self.output, "w") as text_file:
            for element in list_failed_registration:
                text_file.write(element + "\n")


# function defined for setup.cfg
def main():
    script = CheckRequests()
    script()


# make it able to be run from a shell
if __name__ == "__main__":
    main()
