from dataclasses import dataclass

@dataclass
class RunDetails:
    run_number: int
    run_letter: str = ""
    type: str = "" # water or air
    FHC_or_RHC: str = ""


def get_t2k_run_period(run, sub_run) -> RunDetails:
    """
    Return the T2K run period based on the MIDAS run and sub-run numbers
    The information is extracted from https:#git.t2k.org/nd280/highland2Software/psyche/psycheND280Utils/-/blob/master/src/ND280AnalysisUtils.cxx?ref_type=heads
    and https:#t2k.org/nd280/datacomp/production/production007/rdp/7E_13.28
    :param run: MIDAS run number
    :param sub_run: MIDAS sub-run number
    :return: RunDetails
    """

    if run < 6000:                      return RunDetails(1,type="water") # run1  water
    elif run >= 6462 and run < 7664:  return RunDetails(2, type="water") # run2  water
    elif run >= 7665 and run < 7755:  return RunDetails(2, type="air") # run2  air
    elif run >= 8000 and run < 8550:  return RunDetails(3, run_letter="b", type="air") # run3b air = > mc associated to be the one of run2
    elif run >= 8550 and run < 8800:  return RunDetails(3, run_letter="c", type="air") # run3c air
    elif run >= 8983 and run < 9414:  return RunDetails(4, type="water") # run4  water
    elif run >= 9426 and run < 9800:  return RunDetails(4, type="air") # run4  air
    #
    # run5
    # run5a and run5b    are    both  FHC
    # for now using the same run code (run5a)
    # this is fine because there is no MC(could update run code in future)
    elif run >= 10252 and run < 10282: return RunDetails(5, run_letter="a", FHC_or_RHC="FHC") # run5a FHC
    elif run >= 10282 and run < 10303: return RunDetails(5, run_letter="b", FHC_or_RHC="FHC") # run5b FHC (run5a code)
    elif run >= 10334 and run < 10518: return RunDetails(5, run_letter="c", FHC_or_RHC="RHC") # run5c RHC
    elif run == 10518 and sub_run <= 9: return RunDetails(5, run_letter="c", FHC_or_RHC="RHC") # run5c RHC
    elif run == 10518 and sub_run >= 28: return RunDetails(5, run_letter="b", FHC_or_RHC="FHC") # run5b FHC (run5a code)
    elif run >= 10519 and run <= 10521: return RunDetails(5, run_letter="b", FHC_or_RHC="FHC") # run5b FHC (run5a code)
    # #
    # run6
    elif run >= 10931 and run < 10950: return RunDetails(6, run_letter="a", type="air", FHC_or_RHC="FHC") # run6a air FHC
    elif run >= 11305 and run < 11329: return RunDetails(6, run_letter="a", type="air", FHC_or_RHC="FHC") # run6a air FHC
    elif run >= 11436 and run < 11443: return RunDetails(6, run_letter="a", type="air", FHC_or_RHC="FHC") # run6a air FHC
    elif run >= 10952 and run < 11240: return RunDetails(6, run_letter="b", type="air", FHC_or_RHC="RHC") # run6b air RHC
    elif run >= 11449 and run < 11492: return RunDetails(6, run_letter="c", type="air", FHC_or_RHC="RHC") # run6c air RHC
    elif run >= 11492 and run < 11564: return RunDetails(6, run_letter="d", type="air", FHC_or_RHC="RHC") # run6d air RHC
    elif run >= 11619 and run < 11687: return RunDetails(6, run_letter="e", type="air", FHC_or_RHC="RHC") # run6e air RHC
    elif run == 11687:
        if sub_run < 7:
            return RunDetails(6, run_letter="e", type="air", FHC_or_RHC="RHC") # run6e air RHC
        else: return RunDetails(6, run_letter="f", type="air", FHC_or_RHC="FHC") # run6f air FHC

    #
    # run7
    # run7a and run7c are both FHC
    # for now using the same run code (run7c)
    # this is fine because there is no MC (could update run code in future)
    elif run >= 12064 and run < 12073: return RunDetails(7, run_letter="a", type="water", FHC_or_RHC="FHC") # run7a water FHC(run7c code)
    elif run >= 12080 and run < 12557: return RunDetails(7, run_letter="b", type="water", FHC_or_RHC="FHC") # run7b water RHC
    elif run >= 12563 and run < 12590: return RunDetails(7, run_letter="c", type="water", FHC_or_RHC="FHC") # run7c water FHC
    #
    # run8
    elif run >= 12716 and run < 13158: return RunDetails(8, type="water", FHC_or_RHC="FHC") # run8 water FHC
    elif run >= 13190 and run < 13738: return RunDetails(8, type="air", FHC_or_RHC="FHC") # run8 air   FHC
    #
    # run9
    elif run >= 13846 and run < 13879: return RunDetails(9, run_letter="a", type="water", FHC_or_RHC="FHC") # run9a water FHC
    elif run >= 13888 and run < 14032: return RunDetails(9, run_letter="b", type="water", FHC_or_RHC="RHC") # run9b water RHC
    elif run >= 14036 and run < 14083: return RunDetails(9, run_letter="c", type="water", FHC_or_RHC="RHC") # run9c water RHC
    elif run >= 14147 and run < 14418: return RunDetails(9, run_letter="d", type="water", FHC_or_RHC="RHC") # run9d water RHC

