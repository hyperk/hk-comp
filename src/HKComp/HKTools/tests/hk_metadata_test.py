#!/usr/bin/env python

from DIRAC.Core.Base import Script

Script.initialize()

from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient


def main():
    fcc = FileCatalogClient()

    # show available fields
    print(fcc.getMetadataFields())

    # create a new *file* (-f) index
    # res = fcc.addMetadataField('t2k_metatest', 'INT', '-f')
    # print(res)
    # res = fcc.addMetadataField('nd280_version', 'VARCHAR(24)', '-f')
    # print(res)
    # show available fields
    # print(fcc.getMetadataFields())
    # index a file or two with this metadata
    # fcc.setMetadata("/t2k.org/user/g/mathieu.guigue/meta_1/t1.txt", {"t2k_metatest": 1, "nd280_version": "13.3"})
    # fcc.setMetadata("/t2k.org/user/g/mathieu.guigue/meta_1/t2.txt", {"t2k_metatest": 2, "nd280_version": "13.3"})
    # # fcc.setMetadata("/t2k.org/user/k/sophie.king/meta_1/t3.txt", {"t2k_metatest": 3, "nd280_version": "13.3"})
    # # fcc.setMetadata("/t2k.org/user/k/sophie.king/meta_1/t4.txt", {"t2k_metatest": 4, "nd280_version": "13.3"})
    # # fcc.setMetadata("/t2k.org/user/k/sophie.king/meta_1/t5.txt", {"t2k_metatest": 5, "nd280_version": "13.3"})
    # # fcc.setMetadata("/t2k.org/user/k/sophie.king/meta_1/t6.txt", {"t2k_metatest": 6, "nd280_version": "13.3"})
    print(fcc.findFilesByMetadata({"t2k_metatest": 2}, "/hyperk.org"))
    #
    # print(fcc.findFilesByMetadata({"nd280_version": 13.3}, "/t2k.org"))
    #
    # print(fcc.findFilesByMetadata({"t2k_metatest": 2, "nd280_version": 13.3}, "/t2k.org"))


if __name__ == "__main__":
    main()