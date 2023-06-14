#!/bin/env python

from HKComp.DIRACCore.Utilities.BaseScript import BaseScript
from DIRAC.Core.Utilities.List import breakListIntoChunks

from DIRAC import gLogger

from tqdm import trange
import threading

# dirac requires that we set this
__RCSID__ = '$Id$'


class FindReplicasScript(BaseScript):
    """
    This script will find all the replicas of a list of files, and for each storage element, it will put all the
    found replicas in a separate txt file (therefore some files will appear in 2+ files if there is 2+ replicas).
    If no replica for a file can be found, il will be placed in a separate file.
    """
    switches = [
        ('O:', 'output=', 'File prefix to store results in', None, True),
        ('j:', 'nthreads=', 'Number of threads', 5, False),
        ('n:', 'n_files=', 'Number of files to process', -1, False),
    ]

    arguments = [
        ('input', 'File containing the list of LFN', True)
    ]

    def __init__(self):
        super().__init__()
        self.files_location = dict()

    def main(self):

        from DIRAC.Interfaces.API.Dirac import Dirac

        self.dirac = Dirac()

        self.nthreads = int(self.nthreads)
        self.extract_files()
        self.run_with_threading()

        # Save list of failed file replication
        for SE, file_list in self.files_location.items():
            with open(f"{self.output}_{SE}.txt", "w") as text_file:
                for element in file_list:
                    text_file.write(element + "\n")

    def extract_files(self):
        file = open(self.input, 'r')
        lines = file.read().splitlines()
        n_files = int(self.n_files) if int(self.n_files) > 0 else len(lines)
        self.len_subpack = (n_files // self.nthreads + 1)

        self.subpacks = breakListIntoChunks(lines, self.len_subpack)

    def task(self, n_thread, list_r, files_location):

        from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
        from DIRAC.Interfaces.API.Dirac import Dirac

        fc = FileCatalog()
        dirac = Dirac()

        len_chunk = 50  # taking one file at a time
        n_steps = len(list_r)//len_chunk + 1

        split_list_r = breakListIntoChunks(list_r, len_chunk)
        for iterator in range(n_steps):
            info_thread = f"Thread {n_thread} ({iterator}/{n_steps-1}):\t"
            metaDatas = fc.getReplicas(split_list_r[iterator])
            if (not metaDatas['OK']):
                not_ok_files.append(split_list_r[iterator])
                continue
            for filename, SE_dict in metaDatas['Value']['Successful'].items():
                for SE in SE_dict.keys():
                    if SE not in self.files_location.keys():
                        self.files_location.update({str(SE): []})
                    self.files_location[str(SE)].append(filename)
            for filename, SE_dict in metaDatas['Value']['Failed'].items():
                self.files_location["not_ok"].append(filename)
            gLogger.always(f"{info_thread}Check finished")

    def run_with_threading(self):
        self.files_location = dict()
        self.files_location.update({"not_ok": []})
        self.threads = []
        for i in trange(self.nthreads):
            t = threading.Thread(target=self.task, args=(i, self.subpacks[i], self.files_location))
            t.start()
            self.threads.append(t)
        for t in self.threads:
            t.join()


# function defined for setup.cfg
def main():
    script = FindReplicasScript()
    script()


# make it able to be run from a shell
if __name__ == "__main__":
    main()
