#!/bin/env python

from HKComp.Interfaces.Utilities.BaseScript import BaseScript
from HKComp.Interfaces.Utilities.gfalUtils import bringOnline

from tqdm import trange
import threading
import gfal2

# dirac requires that we set this
__RCSID__ = '$Id$'


class BringOnlineAndReplicateScript(BaseScript):
    """
    This script brings files online from a source SE and then replicate this file to the target SR in a multithreading
    manner. In each thread, the files are brought online using gfal2.context.bring_online method one by one.
    Several checks are being made to make sure the files are not already on the target SE and are present on the source
    SE. The list of files to replicate must be provided as a list in a <input> file. Given the slowness of the
    bring-online mechanism, several hundreds of threads are recommended (using the -j switch).
    """
    switches = [
        ('O:', 'output=', 'File to store results in', None, True),
        ('n:', 'n_files=', 'Number of files to process', -1, False),
        ('j:', 'nthreads=', 'Number of threads', 5, False),
        ('S:', 'source=', 'SE where the files should be brought online and replicated from', None, True),
        ('T:', 'target=', 'SE where to replicate the files', None, True),
    ]

    arguments = [
        ('input', 'File containing the list of LFN', True)
    ]

    def __init__(self):
        super().__init__()
        self.ok_files = []
        self.not_ok_files = []

    def main(self):

        from DIRAC import gLogger, exit as DIRAC_exit
        from DIRAC.Interfaces.API.Dirac import Dirac
        from DIRAC import S_OK, S_ERROR, gLogger

        self.dirac = Dirac()

        self.nthreads = int(self.nthreads)
        self.extract_files()
        self.run_with_threading()

        gLogger.always(f"OK files {self.ok_files}")
        gLogger.always(f"NOT OK files {self.not_ok_files}")

        # Save list of failed file replication
        with open("not_ok_"+self.output, "w") as text_file:
            for element in self.not_ok_files:
                text_file.write(element + "\n")

        # Save list of successful file replication
        with open("ok_"+self.output, "w") as text_file:
            for element in self.ok_files:
                text_file.write(element + "\n")

    def extract_files(self):

        from DIRAC.Core.Utilities.List import breakListIntoChunks

        file = open(self.input, 'r')
        lines = file.read().splitlines()
        n_files = self.n_files if self.n_files>0 else len(lines)
        self.len_subpack = ( n_files // self.nthreads +1)

        self.subpacks = breakListIntoChunks(lines, self.len_subpack)

    def task(self, n_thread, list_r, ok_files, not_ok_files):

        from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
        from DIRAC.Interfaces.API.Dirac import Dirac
        from DIRAC import S_OK, S_ERROR, gLogger
        import errno
        import time


        context = gfal2.creat_context()
        fc = FileCatalog()
        dirac = Dirac()

        len_chunk = 1 # taking one file at a time
        # n_steps = len(list_r)//len_chunk + 1
        n_steps = len(list_r)
        # progress_bar = trange(n_steps)
        # progress_bar.set_description(f"Thread {n} -> {len(list_r)}")

        # split_list_r = breakListIntoChunks(list_r, len_chunk)
        for iterator in range(len(list_r)):
            info_thread = f"Thread {n_thread} ({iterator}/{n_steps}):\t"
            gLogger.always(f"{info_thread}Replication of {list_r[iterator]}...")
            try:
                metaDatas = fc.getReplicas(list_r[iterator])
                if (not metaDatas['OK']):
                    not_ok_files.append(list_r[iterator])
                    continue
                if self.target in metaDatas['Value']['Successful'][list_r[iterator]].keys():
                    gLogger.info(f"{info_thread}File {list_r[iterator]} already on target SE")
                    ok_files.append(list_r[iterator])
                    continue
                if self.source not in metaDatas['Value']['Successful'][list_r[iterator]].keys():
                    gLogger.error(f"{info_thread}SE {self.source} not valid source SE")
                    not_ok_files.append(list_r[iterator])
                    continue
                metaDatas = dirac.getAccessURL(list_r[iterator], self.source)
                if (not metaDatas['OK']):
                    gLogger.info(f"{info_thread}Something weird happened: found the file {list_r[iterator]} in FC, but couldn't retrieve the URL!")
                    not_ok_files.append(list_r[iterator])
                    continue
                url = metaDatas['Value']['Successful'][list_r[iterator]]
                # use custom bringOnline function
                bringOnline(context, url, info_thread)

                # exit(1)
                ok = dirac.replicateFile(list_r[iterator], self.target, self.source)
                if not ok['OK']:
                    gLogger.error(f"{info_thread}Replication of {list_r[iterator]} failed")
                    not_ok_files.append(list_r[iterator])
                    continue
                gLogger.always(f"{info_thread}Replication of {url} successful")
                ok_files.append(list_r[iterator])
            except gfal2.GError as error:
                gLogger.error(f"{info_thread}Replication of {url} failed because of gfal error")
                not_ok_files.append(list_r[iterator])
                continue

    def run_with_threading(self):
        self.ok_files = []
        self.not_ok_files = []
        self.threads =[]
        for i in trange(self.nthreads):
            t = threading.Thread(target=self.task, args=(i, self.subpacks[i], self.ok_files, self.not_ok_files ))
            t.start()
            self.threads.append(t)
        for t in self.threads:
            t.join()


# function defined for setup.cfg
def main():
    script = BringOnlineAndReplicateScript()
    script()

# make it able to be run from a shell
if __name__ == "__main__":
    main()
