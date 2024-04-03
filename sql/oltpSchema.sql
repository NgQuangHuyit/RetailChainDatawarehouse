CREATE DATABASE IF NOT EXISTS QuanLyBanHang;
USE QuanLyBanHang;

CREATE TABLE country (
  countryID SMALLINT UNSIGNED  ,
  country VARCHAR(50) ,
  PRIMARY KEY  (countryID)
);

CREATE TABLE city (
  cityID SMALLINT UNSIGNED  ,
  city VARCHAR(50) ,
  countryID SMALLINT UNSIGNED ,
  PRIMARY KEY  (cityID),
  CONSTRAINT `fk_city_country` FOREIGN KEY (countryID) REFERENCES country (countryID) ON DELETE RESTRICT ON UPDATE CASCADE
);


CREATE TABLE address (
  addressID SMALLINT UNSIGNED  ,
  address VARCHAR(50) ,
  address2 VARCHAR(50) DEFAULT NULL,
  district VARCHAR(20) ,
  cityID SMALLINT UNSIGNED ,
  postalCode VARCHAR(10) DEFAULT NULL,
  PRIMARY KEY  (addressID),
  CONSTRAINT `fk_address_city` FOREIGN KEY (cityID) REFERENCES city (cityID) ON DELETE RESTRICT ON UPDATE CASCADE
);


CREATE TABLE color (
	colorID VARCHAR(10) ,
	colorName VARCHAR(50) ,
	rgbCode  VARCHAR(20),
	hexCode VARCHAR(15),
	PRIMARY KEY (colorID)
);

CREATE TABLE category (
	categoryID VARCHAR(10) ,
	categoryName VARCHAR(50) ,
	parentCategoryID VARCHAR(10),
	PRIMARY KEY (categoryID)
);

AlTER TABLE category ADD CONSTRAINT FOREIGN KEY (parentCategoryID) REFERENCES category(categoryID);

CREATE TABLE products (
	productID VARCHAR(10) PRIMARY KEY,
	productName VARCHAR(100),
	productDescription TEXT,
	originalPrice DECIMAL(10,2),
	sellingPrice DECIMAL(10,2),
	avail VARCHAR(10),
	productSize VARCHAR(5),
	productLine VARCHAR(50),
	colorID VARCHAR(10),
	categoryID VARCHAR(10),
	FOREIGN KEY (categoryID) REFERENCES category(categoryID),
	FOREIGN KEY (colorID) REFERENCES color(colorID)
);

CREATE TABLE customers (
	customerID VARCHAR(20) PRIMARY KEY,
	firstName VARCHAR(50),
	lastName VARCHAR(50),
	phone VARCHAR(40),
	email VARCHAR(50),
	cityID SMALLINT UNSIGNED ,
    FOREIGN KEY (cityID) REFERENCES city(cityID)
);

CREATE TABLE promotions (
	promotionID VARCHAR(10) PRIMARY KEY,
	promotionName VARCHAR(50),
	promotionDescription TEXT,
	startDate DATE,
	endDate DATE,
	adsMediaType VARCHAR(50),
	promotionType VARCHAR(50)
);


CREATE TABLE branches (
	branchID VARCHAR(10) PRIMARY KEY,
	branchName VARCHAR(50),
	phone VARCHAR(40),
	email VARCHAR(50),
	addressID SMALLINT UNSIGNED,
	FOREIGN KEY (addressID) REFERENCES address(addressID)
);


CREATE TABLE employees (
	employeeID VARCHAR(10) PRIMARY KEY,
	firstName VARCHAR(50),
	lastName VARCHAR(50),
	email VARCHAR(50),
	phone VARCHAR(40),
	position VARCHAR(50),
	hireDate DATE,
	managerID VARCHAR(10),
    branchID VARCHAR(10),
	FOREIGN KEY (branchID) REFERENCES branches(branchID),
	FOREIGN KEY (managerID) REFERENCES employees(employeeID)
);

CREATE TABLE saleOrders (
	orderID VARCHAR(30) PRIMARY KEY,
	promotionID VARCHAR(10),
	customerID VARCHAR(20),
	employeeID VARCHAR(10),
	branchID VARCHAR(10),
	orderDate DATE,
	totalAmount DECIMAL(25,2),
	FOREIGN KEY (promotionID) REFERENCES promotions(promotionID),
	FOREIGN KEY (customerID) REFERENCES customers(customerID),
	FOREIGN KEY (employeeID) REFERENCES employees(employeeID),
	FOREIGN KEY (branchID) REFERENCES branches(branchID)
);

CREATE TABLE orderDetails (
	orderDetailID INT PRIMARY KEY,
	orderID VARCHAR(30),
	productID VARCHAR(10),
	quantity INT,
	discount DECIMAL(5,2),
	unitPrice DECIMAL(15,2),
	subTotal DECIMAL(20,2),
	FOREIGN KEY (orderID) REFERENCES saleOrders(orderID),
	FOREIGN KEY (productID) REFERENCES products(productID)
);

