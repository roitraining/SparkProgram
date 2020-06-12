CREATE TABLE Regions(
RegionID int,
RegionName string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/class/datasets/northwind/CSV/regions' OVERWRITE INTO TABLE regions;

select * from regions

