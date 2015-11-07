def get_files_in_input_dir():
    import sys
    from os import walk
    path = sys.argv[0].split('/')
    path.pop(len(path)-1)
    path.append("input")
    path = '/'.join(path)
    f = []
    for (dirpath, dirnames, filenames) in walk(path):
        f.extend(filenames)
    return f