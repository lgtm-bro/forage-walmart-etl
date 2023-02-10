# from extract_data import read_file

# data1 = read_file("data/shipping_data_1.csv")
# data2 = read_file("data/shipping_data_2.csv")

def aggregate_shipments(data):
    shipments = {}
    for s in data.values():
        order = s[0][1]
        product= s[1][1]
        if shipments.get(order):
            if shipments[order].get(product):
                shipments[order][product] += 1
            else: shipments[order][product] = 1
        else: shipments[order] = {product: 1}

    return shipments

# s = aggregate_shipments(data1)

def merge_data(data1, data2):

    res = []
    for k, v in data1.items():
        for shipment in data2.values():
            if k == shipment[0][1]:
                for item, quantity in v.items():
                    res.append([ item, quantity, shipment[1][1], shipment[2][1]])

    return res

# r = merge_data(s, data2)
