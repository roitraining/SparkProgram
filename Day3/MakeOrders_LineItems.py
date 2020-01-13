o = spark.read.csv('/class/datasets/northwind/CSVHeaders/orders', header = True, inferSchema = True)
od = spark.read.csv('/class/datasets/northwind/CSVHeaders/orderdetails', header = True, inferSchema = True)

o.createOrReplaceTempView('Orders')
od.createOrReplaceTempView('OrderDetails')
sql = """
select o.OrderID, o.CustomerID, o.OrderDate
           , COLLECT_SET(NAMED_STRUCT("ProductID", od.ProductID, 
                                      "UnitPrice", od.UnitPrice,
                                      "Quantity", od.Quantity,
                                      "Discount", od.discount)) as LineItems
from Orders as o join OrderDetails as od on o.OrderID = od.OrderID
GROUP BY o.OrderID, o.CustomerID, o.OrderDate
ORDER BY o.OrderID"""
od2 = spark.sql(sql)
od2.write.json('Orders_LineItems.json')

