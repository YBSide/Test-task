def compare_versions(version_a, version_b):
    a_tuple = tuple(filter(None, version_a.split('.')))
    b_tuple = tuple(filter(None, version_b.split('.')))

    if a_tuple > b_tuple:
        return 1
    elif a_tuple < b_tuple:
        return -1
    return 0