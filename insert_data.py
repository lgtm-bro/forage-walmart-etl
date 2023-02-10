import sqlite3
from sqlite3 import Error
from extract_data import conform_data


def create_connection(db_file):
    """takes a list of shipment data and inserts it into the product table and shipment table
    :param db_file: db_connection, list of shipment data
    :return: None
    """


    connection = None
    try:
        connection = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return connection


def insert_data(conn, data):
    """transforms csv file data into a hash map and inserts data into SQL database
        with each key as a row and each value as a list of (field, entry) tuples
    :param db_file: csv file
    :return: None
    """

    cursor = conn.cursor()

    for row in data:
        product_id = None

        cursor.execute(
            "INSERT OR IGNORE INTO product (name) VALUES (?)", [row[0]])
        product_id = cursor.lastrowid

        cursor.execute("""INSERT INTO shipment (product_id, quantity, origin, destination)
        values(?, ?, ?, ?)""",
                       [product_id, *row[1:]])


file0_data = conform_data("data/shipping_data_0.csv")
file1_data = conform_data("data/shipping_data_1.csv")

db_conn = create_connection("shipment_database.db")
with db_conn:
    insert_data(db_conn, file0_data)
    insert_data(db_conn, file1_data)