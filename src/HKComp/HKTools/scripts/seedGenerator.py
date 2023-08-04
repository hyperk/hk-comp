#!/usr/bin/env python3

from hashlib import sha1

def GetRandomSeed(run='', subrun='', stage='', hexbits=8, model='', test = False):
    """ Get a random seed from has of run + subrun + stage, nBits"""
    run = str(run)
    subrun = str(subrun)
    stage = str(stage)
    model = str(model)
    combine = (run+subrun+stage+model).encode()
    print('Making seed from:  ', combine)
    h = sha1(combine)
    # .encode will use UTF-8 if not specified
    """
    32 bit unsigned integer -> 8 hexbits:
           max = 0xFFFFFFFF = 2^32-1 = 4294967295 : nd280Control  (10)
    32 bit signed   integer -> 7 hexbits:
           max = 0xFFFFFFF = 2^31-1 = 268435455  : nd280MC, neut  (9)
    """
    bits = int(h.hexdigest()[:hexbits], 16)

    if(test):
        print('(run+subrun+stage+model).encode()               ', (run+subrun+stage+model).encode() )
        print('h = sha1((run+subrun+stage+model).encode())     ' , (sha1((run+subrun+stage+model).encode()) ) )
        print('h.hexdigest()                             ', h.hexdigest() )
        print('h.hexdigest()[:hexbits]                   ', h.hexdigest()[:hexbits] )
        print('int(h.hexdigest()[:hexbits], 16)          ', int(h.hexdigest()[:hexbits], 16) , '\n')

    print( 'Random seed for ' + run + ' ' + subrun + ' ' + stage + ' ' + model + ' is ' + str(bits) )
    return bits



# Useful to have this as a script, as we often need to know the seed to be able to replicate and bug hunt locally
# And if the grid job fails, you don't always have the logs to grab the info.
if __name__ == "__main__":

    GetRandomSeed('90410000', '0044', 'agh', 8, '', True)
    GetRandomSeed('2', '44', 'wcs', 7, 'warren2020_NO_10kpc_a1.25_m10.5', True)
