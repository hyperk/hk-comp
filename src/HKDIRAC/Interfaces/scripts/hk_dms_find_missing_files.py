#!/bin/env python


from HKDIRAC.Interfaces.Utilities.BaseScript import BaseScript

from tqdm import trange
import threading

# dirac requires that we set this
__RCSID__ = '$Id$'


class FindMissingFiles(BaseScript):
    '''
    '''
    switches = [
        ('O:', 'output=', 'YAML file to store results in', None),
        ('i:', 'input=', 'YAML file to read the files from', None),
        ('j:', 'nthreads=', 'Number of threads', 5),
        ('S:', 'se=', 'SE to check', None),
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
        self.lock = threading.Lock()
        self.reader = None
        if self.se is None:
            gLogger.error("No Storage Element (-S) provided: exiting")
            DIRAC_exit(1)
        if self.output is None:
            gLogger.error("No output filename (-O) provided: exiting")
            DIRAC_exit(1)
        if self.input is None:
            gLogger.error("No intput filename (-i) provided: exiting")
            DIRAC_exit(1)

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
