categories = spark.read.csv('/home/student/ROI/SparkProgram/datasets/northwind/CSVHeaders/categories', header=True, inferSchema = True)
print(categories)
categories.show(10)
categories.createOrReplaceTempView('categories') 

products = spark.read.json('/home/student/ROI/SparkProgram/datasets/northwind/JSON/products')
print(products)
products.show()
products.createOrReplaceTempView('products') 

