# HKDirac

## Standard Procedures

### Find the missing files from a Storage Element

```bash
dirac-dms-find-lfns Path=/t2k.org/nd280/raw/ND280 > t2k.org_nd280_raw_ND280.list
hk-dms-find-missing-files -S IN2P3-CC-XRD-disk -j5 -O rawdata_firstpass.txt -i t2k.org_nd280_raw_ND280.list
```
