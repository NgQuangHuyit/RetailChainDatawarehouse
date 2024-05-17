from contextlib import contextmanager
from config import read_config_file
from tqdm import tqdm
from faker import Faker
from typing import List
from Utils import randomDateFromTo
from datetime import date
from mysql.connector import MySQLConnection
from CustomProvider import EmployeeProvider, PromotionProvider
import random
import csv


class DatabaseConnector:
    def __init__(self, **conn_parrams):
        self.host = conn_parrams['host'] 
        self.port = conn_parrams["port"]
        self.user = conn_parrams["user"]
        self.password = conn_parrams["password"]
        self.database = conn_parrams["database"]

    @contextmanager
    def managed_cursor(self):
        self.conn = MySQLConnection(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database)
        self.conn.autocommit = False
        self.cursor = self.conn.cursor()
        try:
            yield self.cursor
        finally:
            self.cursor.close()
            self.conn.commit()
            self.conn.close()


def loadToDatabase(data: List[tuple], insert_query):
    with DatabaseConnector(**read_config_file()).managed_cursor() as cursor:
        for record in tqdm(data):
            cursor.execute(operation=insert_query,params=record)


def readTableColumns(table_name : str, column_names: List[str], filterCondition=1):
    with DatabaseConnector(**read_config_file()).managed_cursor() as cursor:
        cursor.execute(f"SELECT {','.join(column_names)} FROM {table_name} WHERE {filterCondition}")
        rows = cursor.fetchall()
        return rows


fake = Faker()
fake.add_provider(PromotionProvider)


def genPromtionData(records_number=100): 
    with DatabaseConnector(**read_config_file()).managed_cursor() as cursor:
        for _ in tqdm(range(records_number)):
            id = "P{:0>5}".format(_ + 1)
            name = "Promotion {}".format(_)
            description = ""
            start_date = randomDateFromTo(start=date(2021, 1, 1), end=date(2023, 12, 31))
            end_date = randomDateFromTo(start=start_date, end=date(2023, 12, 31))
            ads_media_type = fake.adsMediaTypes()
            promotion_type = fake.promotionType()
            cursor.execute("""
                           INSERT INTO promotions 
                            (
                                promotionID, 
                                promotionName,
                                promotionDescription,
                                startDate,
                                endDate,
                                adsMediaType,
                                promotionType
                            )
                           VALUES (%s, %s, %s, %s, %s, %s, %s)
                           ON DUPLICATE KEY UPDATE
                                promotionName = VALUES(promotionName),
                                promotionDescription = VALUES(promotionDescription),
                                startDate = VALUES(startDate),
                                endDate = VALUES(endDate),
                                adsMediaType = VALUES(adsMediaType),
                                promotionType = VALUES(promotionType)
                           """, 
                           (id, 
                            name, 
                            description, 
                            start_date.strftime("%Y-%m-%d"), 
                            end_date.strftime("%Y-%m-%d"), 
                            ads_media_type, 
                            promotion_type))


def genCustomerData(records_number=1000, chunk_size=1000):
    ls = []
    print("Generating Customer Data")
    for _ in tqdm(range(records_number)):
        id = "CUST{:0>7}".format(_ + 1)
        fname, lname = fake.name().split(maxsplit=1)
        phone_number = fake.phone_number()
        email = fake.email()
        cityID = random.choice(range(1, 601))
        ls.append(tuple([id, fname, lname, phone_number, email, cityID]))
    print("Inserting Customer Data")
    for i in tqdm(range(0, len(ls), chunk_size)):
        with DatabaseConnector(**read_config_file()).managed_cursor() as cursor:
            cursor.executemany("""
                           INSERT INTO customers (
                                customerID,
                                firstName,
                                lastName,
                                phone,
                                email,
                                cityID
                           )
                           VALUES (%s, %s, %s, %s, %s, %s)
                           ON DUPLICATE KEY UPDATE
                                firstName = VALUES(firstName),
                                lastName = VALUES(lastName),
                                phone = VALUES(phone),
                                email = VALUES(email),
                                cityID = VALUES(cityID)""", 
                           ls[i:i+chunk_size])

def genBranchesData(records_number=600):
    with DatabaseConnector(**read_config_file()).managed_cursor() as cursor:
        for _ in tqdm(range(records_number)):
            id = "BR{:0>5}".format(_ + 1)
            name = fake.company()
            phone_number = fake.phone_number()
            email = fake.email()
            addressID = _ + 1
            cursor.execute("""
                           INSERT INTO branches (
                                branchID,
                                branchName,
                                phone,
                                email,
                                addressID 
                           )
                           VALUES (%s, %s, %s, %s, %s)
                           ON DUPLICATE KEY UPDATE 
                                branchName = VALUES(branchName),
                                phone = VALUES(phone),
                                email = VALUES(email),
                                addressID = VALUES(addressID)""",
                           (id,
                            name,
                            phone_number,
                            email,
                            addressID))

fake.add_provider(EmployeeProvider)

def genEmployeeData():
    branchIDs= [rows[0] for rows in readTableColumns("branches", ["branchID"])]
    cnt = 1
    with DatabaseConnector(**read_config_file()).managed_cursor() as cursor:
        for branchID in tqdm(branchIDs):
            ls=[]
            storeManagerID = "EMP{:0>5}".format(cnt)
            cnt += 1
            fname, lname = fake.name().split(maxsplit=1)
            email = fake.email()
            phone_number = fake.phone_number()
            position = "Store Manager"
            hireDate = randomDateFromTo(start=date(2019, 1, 1), end=date(2021, 1, 1))
            managerID = None
            branchID = branchID
            cursor.execute("""
                           INSERT INTO employees (
                                employeeID,
                                firstName,
                                lastName,
                                email,
                                phone,
                                position,
                                hireDate,
                                managerID,
                                branchID
                           )
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                           ON DUPLICATE KEY UPDATE
                                firstName = VALUES(firstName),
                                lastName = VALUES(lastName),
                                email = VALUES(email),
                                phone = VALUES(phone),
                                position = VALUES(position),
                                hireDate = VALUES(hireDate),
                                managerID = VALUES(managerID),
                                branchID = VALUES(branchID)
                                """,
                                (storeManagerID, 
                                 fname, 
                                 lname, 
                                 email, 
                                 phone_number, 
                                 position, 
                                 hireDate.strftime("%Y-%m-%d"), 
                                 managerID, 
                                 branchID))
            for _ in range(50):
                id = "EMP{:0>5}".format(cnt)
                cnt += 1
                fname, lname = fake.name().split(maxsplit=1)
                email = fake.email()
                phone_number = fake.phone_number()
                position = fake.jobTitle()
                hireDate = randomDateFromTo(start=date(2019, 1, 1), end=date(2021, 1, 1))
                managerID = storeManagerID
                branchID = branchID
                ls.append(tuple([id, fname, lname, email, phone_number, position, hireDate.strftime("%Y-%m-%d"), managerID, branchID]))
            cursor.executemany("""
                           INSERT INTO employees (
                                employeeID,
                                firstName,
                                lastName,
                                email,
                                phone,
                                position,
                                hireDate,
                                managerID,
                                branchID
                           )
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                           ON DUPLICATE KEY UPDATE
                                firstName = VALUES(firstName),
                                lastName = VALUES(lastName),
                                email = VALUES(email),
                                phone = VALUES(phone),
                                position = VALUES(position),
                                hireDate = VALUES(hireDate),
                                managerID = VALUES(managerID),
                                branchID = VALUES(branchID)
                                """,
                                ls)
    return ls

def genOrdersData(records_number=1000, chunks_size=1000):
    customerIDs = [row[0] for row in readTableColumns("customers", ["customerID"])]
    saleEmpIDs = [row[0] for row in readTableColumns("employees", ["employeeID"])]
    branchIDs = [row[0] for row in readTableColumns("branches", ["branchID"])]
    promotionIDs = [row[0] for row in readTableColumns("promotions", ["promotionID"])]
    promotionIDs = promotionIDs + [None] * len(promotionIDs)
    
    orders = []
    print("Generating Orders Data")
    for _ in tqdm(range(records_number)):
        orderID = "OD{:0>8}".format(_ + 1)
        promotionID = random.choice(promotionIDs)
        customerID = random.choice(customerIDs)
        saleEmpID = random.choice(saleEmpIDs)
        branchID = random.choice(branchIDs)
        orderDate = randomDateFromTo(start=date(2021, 1, 1), end=date(2023, 12, 31))
        totalAmt = 0
        orders.append(tuple([orderID, promotionID, customerID, saleEmpID, branchID, orderDate, totalAmt]))
    print("Inserting Orders Data")
    for i in tqdm(range(0, len(orders), chunks_size)):
        with DatabaseConnector(**read_config_file()).managed_cursor() as cursor:
            cursor.executemany("""
                               INSERT INTO saleOrders (
                                    orderID,
                                    promotionID,
                                    customerID,
                                    employeeID,
                                    branchID,
                                    orderDate,
                                    totalAmount
                               )
                               VALUES (%s, %s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE
                                      promotionID = VALUES(promotionID),
                                      customerID = VALUES(customerID),
                                      employeeID = VALUES(employeeID),
                                      branchID = VALUES(branchID),
                                      orderDate = VALUES(orderDate),
                                      totalAmount = VALUES(totalAmount)
                               """, 
                               orders[i:i+chunks_size])

        

def genOrderDetailsData(chunk_size=1000):
    orderDetails = []
    productIDs = [row[0] for row in readTableColumns("products", ["productID"])]
    productPrice = {}
    orderIDs = [row[0] for row in readTableColumns("saleOrders", ["orderID"])]
    for row in readTableColumns("products", ["productID",  "sellingPrice"]):
        productPrice[row[0]] = float(row[1])
    orderTotal = []
    for orderID in tqdm(orderIDs):
        totalAmt = 0
        for __ in range(random.randint(1, 10)):
            orderDetailID = len(orderDetails) + 1
            productID = random.choice(productIDs)
            quantity = random.randint(1, 10)
            unitPrice = productPrice[productID]
            discount = random.choices([0, 0.05, 0.1, 0.15, 0.2, 0.3, 0.4], weights=[50, 10, 10, 10, 10, 5, 5], k=1)[0]
            subTotal = unitPrice * quantity * (1 - discount)
            totalAmt += subTotal
            orderDetails.append(tuple([orderDetailID, orderID, productID, quantity,discount, unitPrice, subTotal]))
        orderTotal.append(tuple([totalAmt, orderID]))
    for i in tqdm(range(0, len(orderDetails), chunk_size)):
        with DatabaseConnector(**read_config_file()).managed_cursor() as cursor:
            cursor.executemany("""
                               INSERT INTO orderDetails (
                                    orderDetailID,
                                    orderID,
                                    productID,
                                    quantity,
                                    discount,
                                    unitPrice,
                                    subTotal)
                               VALUES (%s, %s, %s, %s, %s, %s, %s)
                               ON DUPLICATE KEY UPDATE
                                    orderID = VALUES(orderID),
                                    productID = VALUES(productID),
                                    quantity = VALUES(quantity),
                                    discount = VALUES(discount),
                                    unitPrice = VALUES(unitPrice),
                                    subTotal = VALUES(subTotal)""", 
                               orderDetails[i:i+chunk_size])
    for i in tqdm(range(0, len(orderTotal), chunk_size)):
        with DatabaseConnector(**read_config_file()).managed_cursor() as cursor:
            cursor.executemany("""
                               UPDATE saleOrders
                               SET totalAmount = %s
                               WHERE orderID = %s
                               """,orderTotal[i:i+chunk_size])


def loadCsvToDatabase(file_path, insert_query, skip_header=True):
    with DatabaseConnector(**read_config_file()).managed_cursor() as cursor:
        with open(file_path, "r") as file:
            reader = csv.reader(file)
            for row in tqdm(reader):
                row = [value if value != "" else None for value in row]
                if skip_header:
                    skip_header = False
                    continue
                cursor.execute(operation=insert_query, params=row)

insertColor = """
    INSERT INTO color (
        colorID,
        colorName,
        rgbCode,
        hexCode
    )
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        colorName = VALUES(colorName),
        rgbCode = VALUES(rgbCode),
        hexCode = VALUES(hexCode)
    """

insertCategory = """
    INSERT INTO category (
        categoryID,
        categoryName,
        parentCategoryID
    )
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
        categoryName = VALUES(categoryName),
        parentCategoryID = VALUES(parentCategoryID)
    """

insertProduct = """
    INSERT INTO products (
        productID,
        productName,
        productDescription,
        originalPrice,
        sellingPrice,
        avail,
        productSize,
        productLine,
        colorID,
        categoryID
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        productName = VALUES(productName),
        productDescription = VALUES(productDescription),
        originalPrice = VALUES(originalPrice),
        sellingPrice = VALUES(sellingPrice),
        avail = VALUES(avail),
        productSize = VALUES(productSize),
        productLine = VALUES(productLine),
        colorID = VALUES(colorID),
        categoryID = VALUES(categoryID)
    """

insertCountry = """
    INSERT INTO country (
        countryID,
        country
    )
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE
        country = VALUES(country)
    """

insertCity = """
    INSERT INTO city (
        cityID,
        city,
        countryID
    )
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
        city = VALUES(city),
        countryID = VALUES(countryID)
    """

insertAddress = """
    INSERT INTO address (
        addressID,
        address,
        address2,
        district,
        cityID,
        postalCode
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        address = VALUES(address),
        address2 = VALUES(address2),
        district = VALUES(district),
        cityID = VALUES(cityID),
        postalCode = VALUES(postalCode)
    """


if __name__ == "__main__":
    loadCsvToDatabase("./StaticDataset/colorTbl.csv", insertColor)
    loadCsvToDatabase("./StaticDataset/categoryTbl.csv", insertCategory)
    loadCsvToDatabase("./StaticDataset/productTbl.csv", insertProduct)
    loadCsvToDatabase("./StaticDataset/countryTbl.csv", insertCountry)
    loadCsvToDatabase("./StaticDataset/cityTbl.csv", insertCity)
    loadCsvToDatabase("./StaticDataset/addressTbl.csv", insertAddress)

    genPromtionData(1000)
    genCustomerData(100000, chunk_size=100000)
    genBranchesData()
    genEmployeeData()
    genOrdersData(200000, chunks_size=100000)
    genOrderDetailsData(chunk_size=100000)