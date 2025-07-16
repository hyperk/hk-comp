# HKDirac

## Use docker container

For convenience and avoiding general headaches, `docker compose` can be used to have a working system very easily.
To start this, you would need to have `docker` and `docker compose` installed (you could also use singularity, but it hasn't)
been tested).
Then run in this `hk-comp` directory:

```bash
docker compose run --build dirac-container
```

This will first build the docker image, create a container with this image and finally mount your certificates under 
`/root/.globus` and your home directory under `/host` (you can change this mounting policy by editing the `docker-compose.yaml`
file).
During this process, you will be asked for your certificates password twice (one for the initial initialisation of the proxy
and one for the hyper.org_user proxy creation).
You can change the group to join (e.g. `hyperk.org_user` or `hyperk.org_prod`) by editing the `docker-entrypoint.sh` file.

After this procedure, you should see a change in your shell prompt: you should then have access to the dirac command 
along with the `hk-XXX` ones that are defined in this `hk-comp` repository (see below).

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
