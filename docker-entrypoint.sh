#!/usr/bin/env bash

source /usr/local/hk/diracos/diracosrc

echo "Configuring GridPP (type your certificate password)"
dirac-proxy-init
dirac-configure -F -S GridPP -C dips://dirac01.grid.hep.ph.ic.ac.uk:9135/Configuration/Server -I

echo "Init proxy for hyperk.org_user (type your certificate password)"
dirac-proxy-init -g hyperk.org_user -M -v 72:00

/bin/bash