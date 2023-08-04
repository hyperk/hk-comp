import os
import tarfile




def search_for_file(contain, notcontain = '', directory = './'):
    """
    Searches for files in a given directory.
    Allows you to specify a string or list of strings that the filename must contain.
    Allows you to specify a string or list of strings that the filename must not contain.
    Returns the files as a list.
    """

    #If the user specifies single strings, we put it into a list
    if isinstance(contain, str) and contain != '':
        contain = [contain]
    if isinstance(notcontain, str) and notcontain != '':
        notcontain = [notcontain]

    found_files = []

    # Iterate over files in the specified directory
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            print(file_path)
            # Check if the file name satisfies the conditions
            if all(substring in file for substring in contain) and all(substring not in file for substring in notcontain):
                found_files.append(file_path)

    print (found_files)
    return found_files




def find_shared_prefix(filenames):
    """
    Provide a list of strings, 'filenames'
    It will work out the prefix, if any, shared by these.
    """

    if not filenames:
        return ""

    shared_prefix = ""
    first_filename = filenames[0]

    #enumerate allows you to loop through elements while providing a counter
    #the first element is the index, i
    #the second element is the corresponding value from the iterable first_filename, chat
    for i, char in enumerate(first_filename):
        for filename in filenames[1:]:
            if i >= len(filename) or filename[i] != char:
                return shared_prefix
        shared_prefix += char

    return shared_prefix





def create_tarball(filenames, tarball_name):
    """
    Provide a list of files, 'filenames', which should include the directory.
    It will then tar them and produce a file, tarball_name.tar.gz
    The tarball will be created in the same directory as the files it tars.
    """
    with tarfile.open(tarball_name, 'w:gz') as tar:
        for filename in filenames:
            tar.add(filename)

    print('Tarball created: '+ tarball_name)






def create_tarball_suffix(directory, suffix='_catalogue.dat', common_prefix=True, out_prefix=''):
    """
    Looks in 'directory' for all files that end in 'suffix'.
    Set 'common_prefix' to True if the files share part of the start of their name.
    It will only select files with a shared prefix and use it in the output name.
    You can also add a new prefix to the output file, 'out_prefix'
    Designed to tar nd280 catalogue files, but intended to be versitile.
    """

    # Get the list of files in the directory
    file_list = os.listdir(directory)

    # Filter the files based on the suffix and include directory path
    relevant_files = [os.path.join(directory, file) for file in file_list if file.endswith(suffix)]

    if not relevant_files:
        print('No relevant files found.')
        return


    # Take part of the suffix as the name to identify it
    clip_suffix = suffix.split('.')[0]
    clip_suffix = clip_suffix.lstrip('_')
    clip_suffix = clip_suffix.rstrip('_')
    clip_suffix = '_' + clip_suffix

    # Determine the tarball name
    if len(relevant_files) == 1:
        tarball_name = os.path.basename(relevant_files[0]).replace(suffix, '') + '.tar.gz'
    else:
        shared_prefix = ""
        if common_prefix:
            shared_prefix = find_shared_prefix([os.path.basename(file) for file in relevant_files])
            relevant_files = [file for file in relevant_files if os.path.basename(file).startswith(shared_prefix)]

        if out_prefix:
            out_prefix += "_"

        if shared_prefix:
            tarball_name = out_prefix + shared_prefix + clip_suffix + '.tar.gz'
        else:
            tarball_name = out_prefix + clip_suffix + '.tar.gz'

    # Include directory path in tarball_name
    tarball_name = os.path.join(directory, tarball_name)

    # Call the create_tarball function to create the tarball
    create_tarball(relevant_files, tarball_name)



