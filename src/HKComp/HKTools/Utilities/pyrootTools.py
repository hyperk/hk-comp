import ROOT

def check_tree_existence(file_name, tree_name, silence_found = False):
    """
    Checks the existance of a Tree (tree_name) in a root file (file_name)
    Option to silence output when the tree is found, i.e. only make noise when the tree is missing
    """

    root_file = ROOT.TFile.Open(file_name)

    if not root_file or root_file.IsZombie():
        print("Error opening file: "+ file_name)
        return False

    tree = root_file.Get(tree_name)
    if not tree:
        print("Tree " + tree_name + "  not found in file: " + file_name)
        return False

    if not silence_found:
      print("Tree "+ tree_name + " found in file" + file_name)

    root_file.Close()
    return True



def check_tree_existence_list(file_names, tree_name, dodgy_files = "dodgy_files.list", silence_found = False):
    """
    Checks the existance of a Tree (tree_name) in a root file (file_name)
    Option to silence output when the tree is found, i.e. only make noise when the tree is missing
    You can feed it a list of files, and it will write all files with tree_name missing to dodgy_files
    """

    # If a single filename is provided, convert it to a list
    if isinstance(file_names, str):
        file_names = [file_names]

    f_out = open('dodgy_files.list', 'w')

    for file_name in file_names:
        tree = check_tree_existence(file_name, tree_name, silence_found)
        if not tree:
            f_out.write(file_name + '\n')

    f_out.close()


