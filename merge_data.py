def aggregate_shipments(data):
    """Takes a hash map of tuples and returns a hash map of shipments
       with items in the same shipment aggregated into one entry
    params: dict of items shipped
    return: dict with shipment id as key and dict of products in shipment with their qnty as a value
    """

    shipments = {}
    for s in data.values():
        order = s[0][1]
        product = s[1][1]
        if shipments.get(order):
            if shipments[order].get(product):
                shipments[order][product] += 1
            else:
                shipments[order][product] = 1
        else:
            shipments[order] = {product: 1}

    return shipments


def merge_data(data1, data2):
    """Takes a hash map of aggregated shipment data and joins it to
        a hash map of shipment ids with origin and destination data
    params: dict of aggregated shipment data, dict of shipment ids
    returns: list of relevant shipment data
    """

    res = []
    for k, v in data1.items():
        for shipment in data2.values():
            if k == shipment[0][1]:
                for item, quantity in v.items():
                    res.append(
                        [item, quantity, shipment[1][1], shipment[2][1]])

    return res