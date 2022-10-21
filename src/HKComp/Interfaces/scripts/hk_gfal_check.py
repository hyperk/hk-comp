#!/bin/env python
"""
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from HKComp.Interfaces.Utilities.BaseScript import BaseScript

from DIRAC import gLogger
import gfal2

from tqdm import trange
import threading


class GFALCheckScript(BaseScript):
    '''
    Execute a gfal.stat on each file in the input file provided in a LFN format e.g. /t2k.org/nd280...
    If the file exists and has proper stats, the filename is placed in the output file e.g. <output>; if not, in a separated file e.g. missing_<output>
    The suffix is put in front of each file in the input file before doing the gfal.stat.

    This script is useful in the case where the transfer did happen, but the registration didn't. This allows one to find
    the missing files and physically remove them from the SE.

    Example:
        hk-gfal-check -O on_tape_prod6T_sep13_2250_sub1.txt -j 10 -S "root://ccxroot.in2p3.fr:1097/xrootd/in2p3.fr/tape/t2k.org" not_ok_prod6T_sep13_2250_sub1.txt
    '''
    switches = [
        ('O:', 'output=', 'File to store results in', None, True),
        ('j:', 'nthreads=', 'Number of threads', 5, False),
        ('S:', 'suffix=', 'Suffix to prepend to files e.g. root://ccxroot.in2p3.fr:1097/xrootd/in2p3.fr/disk/t2k.org',
         None, True),
    ]

    arguments = [
        ('input', 'File containing the list of LFN', True)
    ]

    def __init__(self):
        super().__init__()

    def main(self):

        self.nthreads = int(self.nthreads)
        self.extract_files()
        self.run_with_threading()

        with open(self.output, "w") as text_file:
            for element in self.present_files:
                text_file.write(element + "\n")
        with open("missing_" + self.output, "w") as text_file:
            for element in self.missing_files:
                text_file.write(element + "\n")

    def extract_files(self):
        from DIRAC.Core.Utilities.List import breakListIntoChunks

        file = open(self.input, 'r')
        lines = file.read().splitlines()
        n_files = len(lines)
        self.len_subpack = (n_files // self.nthreads + 1)
        self.subpacks = breakListIntoChunks(lines, self.len_subpack)

    def task(self, n, suffix, list_r, present_files, missing_files):

        context = gfal2.creat_context()

        n_steps = len(list_r)
        progress_bar = trange(n_steps)
        progress_bar.set_description(f"Thread {n} -> {len(list_r)}")

        for iterator in progress_bar:
            try:
                a = context.stat(f"{suffix}{list_r[iterator]}")
                present_files.append(list_r[iterator])
            except gfal2.GError as error:
                missing_files.append(list_r[iterator])
                continue

    def run_with_threading(self):
        self.present_files = []
        self.missing_files = []
        self.threads = []
        for i in trange(self.nthreads):
            name = 'thread {}'.format(i)
            t = threading.Thread(target=self.task,
                                 args=(i, self.suffix, self.subpacks[i], self.present_files, self.missing_files))
            t.start()
            self.threads.append(t)
        for t in self.threads:
            t.join()


# function defined for setup.cfg
def main():
    script = GFALCheckScript()
    script()


# make it able to be run from a shell
if __name__ == "__main__":
    main()
