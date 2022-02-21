#!/bin/env python

from HKDIRAC.Interfaces.Utilities.BaseScript import BaseScript

from tqdm import trange
import threading

# dirac requires that we set this
__RCSID__ = '$Id$'


class HKDMSFindMissingFiles(BaseScript):
    '''
    '''
    switches = [
        ('O:', 'output=', 'YAML file to store results in', None),
        ('i:', 'input=', 'YAML file to read the files from', None),
        ('j:', 'nthreads=', 'Number of threads', 5),
        ('S:', 'se=', 'SE to check', None),
    ]

    def __init__(self):
        self.lock = threading.Lock()
        self.threads = []
        self.not_ok_files = []
        self.ok_files = []

    def main(self):

        from DIRAC import gLogger, exit as DIRAC_exit
        from DIRAC.Interfaces.API.Dirac import Dirac
        from DIRAC import S_OK, S_ERROR, gLogger

        self.dirac = Dirac()
        import os

        self.nthreads = int(self.nthreads)
        if self.se is None:
            gLogger.error("No Storage Element (-S) provided: exiting")
            DIRAC_exit(1)
        if self.output is None:
            gLogger.error("No output filename (-O) provided: exiting")
            DIRAC_exit(1)
        if self.input is None:
            gLogger.error("No intput filename (-i) provided: exiting")
            DIRAC_exit(1)
        if not self.output.endswith(".yaml"):
            gLogger.error("Dont support file format.{}".format(self.output))
            DIRAC_exit(1)

        self.extract_files()
        self.ok_files = []
        self.not_ok_files = []
        self.run_with_threading()

        with open(self.output, "w") as text_file:
            # print(len(self.not_ok_files))
            for element in self.not_ok_files:
                # print(len(a_list))
                # for element in a_list:
                text_file.write(element + "\n")

        with open("ok_file.txt", "w") as text_file:
            # print(len(self.ok_files))
            for element in self.ok_files:
                # print(len(a_list))
                # for element in a_list:
                text_file.write(element + "\n")

    def extract_files(self):

        from DIRAC.Core.Utilities.List import breakListIntoChunks

        file = open(self.input, 'r')
        lines = file.readlines()
        n_files = len(lines)
        self.len_subpack = (n_files // self.nthreads + 1)

        self.subpacks = breakListIntoChunks(lines, self.len_subpack)
        # = [ lines[self.len_subpack*i:self.len_subpack*(i+1)] for i in range(self.nthreads) ]
        # print(len(self.subpacks))
        # for l in self.subpacks:
        #     print(len(l))
        #     print(l[1])
        # print("->", n_files)

    def task(self, n, list_r, ok_files, not_ok_files):

        from DIRAC.Core.Utilities.List import breakListIntoChunks

        len_chunk = 100
        # iterator = 0
        n_steps = len(list_r) // len_chunk + 1
        # while iterator < len(list_r):
        pbar = trange(n_steps)
        pbar.set_description(f"Thread {n} -> {len(list_r)}")
        # counter = 0

        splitted_list_r = breakListIntoChunks(list_r, len_chunk)
        for iterator in pbar:
            result = self.dirac.getReplicas(splitted_list_r[iterator])['Value']
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
        for i in trange(self.nthreads):
            name = 'thread {}'.format(i)
            t = threading.Thread(target=self.task, args=(i, self.subpacks[i], self.ok_files, self.not_ok_files))
            t.start()
            self.threads.append(t)
        for t in self.threads:
            t.join()


# make it able to be run from a shell
def main():
    script = HKDMSFindMissingFiles()
    script()


if __name__ == "__main__":
    main()
