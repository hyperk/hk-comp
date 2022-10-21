#!/bin/env python

from HKComp.Interfaces.Utilities.BaseScript import BaseScript

from tqdm import trange
import threading

# dirac requires that we set this
__RCSID__ = '$Id$'


class FindMissingFiles(BaseScript):
    '''
    '''
    switches = [
        ('O:', 'output=', 'File to store results in', None, True),
        ('j:', 'nthreads=', 'Number of threads', 5, False),
        ('S:', 'se=', 'SE to check', None, True),
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

        with open("not_ok_"+self.output, "w") as text_file:
            for element in self.not_ok_files:
                text_file.write(element + "\n")

        with open("ok_"+self.output, "w") as text_file:
            for element in self.ok_files:
                text_file.write(element + "\n")

    def extract_files(self):

        from DIRAC.Core.Utilities.List import breakListIntoChunks

        file = open(self.input, 'r')
        lines = file.read().splitlines()
        n_files = len(lines)
        self.len_subpack = ( n_files // self.nthreads +1)

        self.subpacks = breakListIntoChunks(lines, self.len_subpack)

    def task(self, n, list_r, ok_files, not_ok_files):

        from DIRAC.Core.Utilities.List import breakListIntoChunks

        len_chunk = 100
        n_steps = len(list_r)//len_chunk + 1
        progress_bar = trange(n_steps)
        progress_bar.set_description(f"Thread {n} -> {len(list_r)}")

        split_list_r = breakListIntoChunks(list_r, len_chunk)
        for iterator in progress_bar:
            result = self.dirac.getReplicas(split_list_r[iterator])['Value']
            for file, ses in iter(result["Successful"].items()):
                found_file = False
                for a_se in ses.keys():
                    if a_se == self.se:
                        found_file = True
                        ok_files.append(file)
                        break
                if not found_file:
                    not_ok_files.append(file)
            for file, ses in iter(result["Failed"].items()):
                not_ok_files.append(file)

    def run_with_threading(self):
        self.ok_files = []
        self.not_ok_files = []
        self.threads =[]
        for i in trange(self.nthreads):
            name = 'thread {}'.format(i)
            t = threading.Thread(target=self.task, args=(i, self.subpacks[i], self.ok_files, self.not_ok_files ))
            t.start()
            self.threads.append(t)
        for t in self.threads:
            t.join()


# function defined for setup.cfg
def main():
    script = FindMissingFiles()
    script()

# make it able to be run from a shell
if __name__ == "__main__":
    main()
