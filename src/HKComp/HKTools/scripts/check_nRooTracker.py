#!/usr/bin/env python 

import fileTools
import pyrootTools

# Check the numc file to see that it is created properly with a nRooTracker TTree
# Print True if it has, False if it hasn't
# This will be used to decide if we want to upload the job
# This is designed to run in a single temp job area, such as a grid job, such that we only expect one numc in the directory.

# At the moment, outside of nd280 container, I just use the grid job dirac env.
# So I don't have uproot.
# I therefore run this script inside the nd280 container, using pyROOT, and write the status to a file so my upload jobs outside the nd280 container can read it.
# Can look for a better solution in the longterm when moving to the new system.


isitok = False

files = fileTools.search_for_file(["_numc_", ".root"], ".geo.root", "./")

# We are only expecting to find one file.
# If we find more than one, then we are not using this as or where expected..
if(len(files) > 1):
    print('Too many numc root files in this directory!')
else:
    isitok = pyrootTools.check_tree_existence(files[0], "nRooTracker", True)

f_out = open('permissionToUpload.txt', "w")
f_out.write(str(isitok))

print('')
print('Checking if nRooTracker exists: ' + files[0])
print(isitok)
print('')
