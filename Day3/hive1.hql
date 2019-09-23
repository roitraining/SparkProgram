dfs -mkdir /regions;
dfs -put /examples/northwind/CSVHeaders/regions/regions.csv /regions;

set hive.execution.engine=mr;

CREATE EXTERNAL TABLE Regions(
RegionID int,
RegionName string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/regions';

SELECT * FROM Regions;

SELECT *, INPUT__FILE__NAME FROM Regions;

ALTER TABLE Regions RENAME TO Regions_Table;

CREATE VIEW Regions_View AS SELECT RegionID, LTRIM(RTRIM(RegionName)) AS RegionName FROM Regions_Table WHERE RegionID IS NOT NULL;

SELECT * FROM Regions_View;

CREATE TABLE Regions(
RegionID int,
RegionName string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/examples/northwind/CSV/regions' OVERWRITE INTO TABLE Regions;
SELECT * FROM Regions;

CREATE EXTERNAL TABLE Regions2(
RegionID int,
RegionName string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/regions'
TBLPROPERTIES("skip.header.line.count"="1");

SELECT * FROM Regions2;

CREATE EXTERNAL TABLE Regions_Tab(
RegionID int,
RegionName string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/regions_tab';

INSERT INTO Regions_Tab SELECT * FROM Regions;

SELECT * FROM Regions_Tab;

dfs -cat /regions_tab/*;

ADD JAR /usr/local/hive/hcatalog/share/hcatalog/hive-hcatalog-core.jar;

CREATE TABLE Territories(
TerritoryID string,
TerritoryName string,
RegionID int)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/territories';

LOAD DATA LOCAL INPATH '/examples/northwind/JSON/territories/territories.json' OVERWRITE INTO TABLE Territories;

SELECT * FROM Territories;

CREATE TABLE Categories
(
CategoryID int,
CategoryName string,
Description string
)
STORED AS AVRO;

LOAD DATA LOCAL INPATH '/examples/northwind/AVRO/categories' OVERWRITE INTO TABLE Categories;

SELECT * FROM Categories;

CREATE TABLE Products
(
ProductID int,
ProductName string,
SupplierID int,
CategoryID int,
QuantityPerUnit string,
UnitPrice double,
UnitsInStock int,
UnitsOnOrder int,
ReorderLevel int,
Discontinued boolean
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/examples/northwind/JSON/products/products.json' OVERWRITE INTO TABLE Products;

CREATE TABLE Territories_List
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE
AS
SELECT RegionID, collect_set(TerritoryName) AS TerritoryList
FROM Territories
GROUP BY RegionID;

SELECT * FROM Territories_List;

SELECT t.RegionID, l AS TerritoryName
FROM Territories_List AS t
LATERAL VIEW EXPLODE(TerritoryList) EXPLODED_TABLE AS l;

CREATE TABLE Territories_Complex 
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE
AS
SELECT RegionID
, COLLECT_SET(NAMED_STRUCT("TerritoryID", TerritoryID, "TerritoryName", TerritoryName)) AS TerritoryList
FROM Territories
GROUP BY RegionID;

SELECT * FROM Territories_Complex;

SELECT t.RegionID, l
FROM Territories_Complex AS t
LATERAL VIEW EXPLODE(TerritoryList) EXPLODED_TABLE AS l;

SELECT t.RegionID, l.TerritoryName, l.TerritoryID
FROM Territories_Complex AS t
LATERAL VIEW EXPLODE(territorylist) EXPLODED_TABLE AS l;

CREATE TABLE Person(
PersonID int,
Name string,
Skills ARRAY<string>
)
STORED AS AVRO;

INSERT INTO Person 
SELECT 1, 'joey', ARRAY('Java', 'Python', 'Hadoop') 
UNION ALL SELECT 2, 'mary', ARRAY('C++', 'Java', 'Hive');

SELECT PersonID, Name, Skills FROM Person;
SELECT PersonID, Name, Skills[0], Skills[1] FROM Person;

CREATE TABLE Skills_Denormalized AS
SELECT PersonID, Name, SkillName 
FROM Person LATERAL VIEW EXPLODE(Skills) EXPLODED_TABLE AS SkillName;

SELECT PersonID, Name, COLLECT_SET(SkillName) AS Skills
FROM Skills_Denormalized
GROUP BY PersonID, Name;

CREATE TABLE Transactions
(ID int,
Amount double
)
PARTITIONED BY (Year int);

INSERT INTO Transactions PARTITION (Year=2015) SELECT 1, 10 UNION ALL SELECT 2, 20;
dfs -ls /user/hive/warehouse/transactions;

INSERT INTO Transactions PARTITION (Year=2016) SELECT 3,30 UNION ALL SELECT 4,40 UNION ALL SELECT 5,50;
dfs -ls /user/hive/warehouse/transactions;
  
CREATE TABLE RegionTerritories
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
AS
SELECT r.RegionID, r.RegionName
, COLLECT_SET(NAMED_STRUCT("TerritoryID", TerritoryID, "TerritoryName", TerritoryName)) AS TerritoryList
FROM Regions AS r
JOIN Territories AS t ON r.RegionID = t.RegionID
GROUP BY r.RegionID, r.RegionName
ORDER BY r.RegionID;

dfs -cat /user/hive/warehouse/regionterritories/*;
