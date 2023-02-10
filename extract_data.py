from merge_data import aggregate_shipments, merge_data


def read_file(file):
    """Takes a csv file and creates a hash map of its rows
       with a list of (heading, entry) tuples for each entry
    :param db_file: csv file
    :return: dict with an item for each row in file
    """

    hash_map = {}

    f = open(file)
    headings = f.readline().strip().split(",")

    for i, line in enumerate(f):
        line = line.strip()
        entries = line.split(",")

        hash_map[i] = []
        for idx, e in enumerate(entries):
            hash_map[i].extend([(headings[idx], e)])

    return hash_map


def normalize_data0(file):
    """Takes in csv file of shipment data and returns data relevant to the database in a list
    params: csv file
    returns: list of data relevant to the database
    """
    data = read_file(file)
    res = []

    for entry in data.values():
        res. append([entry[2][1], entry[4][1], entry[0][1], entry[1][1]])

    return res


def conform_data(file):
    """Takes in csv file of shipment data and returns data relevant to the database in a list
    params: csv file
    returns: list of data relevant to the database
    """

    if file == "data/shipping_data_1.csv":
        file0_shipments = aggregate_shipments(read_file(file))
        return merge_data(file0_shipments, read_file("data/shipping_data_2.csv"))

    if file == "data/shipping_data_0.csv":
        return normalize_data0("data/shipping_data_0.csv")