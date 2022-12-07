
import gfal2
import errno
import time
from DIRAC import S_OK, S_ERROR, gLogger

def bringOnline(context, url, info_thread=""):
    surl = [url]  # here we could use a list with several elements instead of one element
    (errors, token) = context.bring_online([url], 3600, 604800, True)

    # Extracted from https://gitlab.cern.ch/dmc/gfal2-util/-/blob/master/src/gfal2_util/tape.py
    def _evaluate_errors(errors, surls, polling):
        n_terminal = 0
        for surl, error in zip(surls, errors):
            if error:
                if error.code != errno.EAGAIN:
                    gLogger.debug(f"{info_thread}{surl} => FAILED: {error.message}")
                    n_terminal += 1
                else:
                    gLogger.debug(f"{info_thread}{surl} QUEUED")
            elif not polling:
                gLogger.info(f"{info_thread}{surl} QUEUED")
            else:
                n_terminal += 1
                gLogger.debug(f"{info_thread}{surl} READY")
        return n_terminal

    n_terminal = _evaluate_errors(errors, surl, polling=False)

    # Start the polling
    wait = 3600
    sleep = 1
    while n_terminal != 1 and wait > 0:
        gLogger.debug(f"{info_thread}Request queued for {url}, sleep {sleep} seconds...")
        wait -= sleep
        time.sleep(sleep)
        errors = context.bring_online_poll(surl, token)
        n_terminal = _evaluate_errors(errors, surl, polling=True)
        sleep *= 2
        sleep = min(sleep, 300)
    if errors[0] is None:
        gLogger.info(f"{info_thread}Finished bringonline request for {url}")