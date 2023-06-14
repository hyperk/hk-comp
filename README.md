# HKDirac

## Standard Procedures

## Setup DIRACOS

## Setup DIRAC proxy

````bash
dirac-proxy-init -g t2k.org_user -M -U -v 72:00
````

### Find the missing files from a Storage Element

```bash
dirac-dms-find-lfns Path=/t2k.org/nd280/raw/ND280 > t2k.org_nd280_raw_ND280.list
hk-dms-find-missing-files -S IN2P3-CC-XRD-disk -j5 -O rawdata_firstpass.txt t2k.org_nd280_raw_ND280.list
```

### Submit requests for replicating files between two locations

The following commands will submit requests for transferring all files under `/t2k.org/nd280/raw/ND280/ND280/00003000_00003999/` to IN2P3-CC-XRD-disk using the input files.
It will NOT check whether the file is already registered on this Storage Element before starting the transfer: you should only choose the files that need transfer using the commands from [here](Find-the-missing-files-from-a-Storage-Element).

```bash
dirac-dms-find-lfns Path=/t2k.org/nd280/raw/ND280 > t2k.org_nd280_raw_ND280.list # grab all files from the DFC
hk-rms-submit-requests -D /t2k.org/nd280/raw/ND280/ND280/00003000_00003999/ -T IN2P3-CC-XRD-disk raw_3000_3999 t2k.org_nd280_raw_ND280.list -d
```
