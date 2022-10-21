'''
    Manipulation of LFN lists
'''


def getLFNList(content, subDir, pattern, number=-1):
    '''
    Filters a list of LFN based on a pattern and if they belong to a directory
    :param content: list of lfns
    :param subDir: sub-directory where the lfn should belong
    :param pattern: inclusion pattern
    :param number: number of lfns from the initial list to process; if =-1, process all
    :return: list of filtered lfn
    '''
    filtered_content = []
    iterator = 0
    for item in content:
        if item.startswith(subDir) and pattern in item:
            filtered_content.append(item.rstrip("\n"))
        iterator +=1
        if number >0 and iterator >= number:
            break
    return filtered_content