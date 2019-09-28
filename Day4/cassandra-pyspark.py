# pip install cassandra-driver
# pyspark --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1

from cassandra.cluster import Cluster
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.execute('DROP KEYSPACE IF EXISTS classroom')
session.execute("CREATE KEYSPACE classroom WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':'1'}")
session = cluster.connect('classroom')
session.execute("create table student(id int PRIMARY KEY, firstname text, lastname text, emails set<text>)")
session.execute("insert into student (id, firstname, lastname, emails) values (1, 'Joe', 'Smith', {'joes@xyz.com', 'joe.smith@abc.net'})")
session.execute("update student set firstname = 'Joseph' where id = 1")
session.execute("insert into student (id, firstname, lastname, emails) values (2, 'Mike', 'Jones', {'mikej@xyz.com', 'mike.jones@def.net', 'mike1234@gmail.com'})")
rows = session.execute('SELECT id, firstname, lastname, emails from student')
print(list(rows))

# Python to access a Cassandra cluster through Spark
people = spark.read.format("org.apache.spark.sql.cassandra").options(table="student", keyspace="classroom").load()
people.show()

# Append the results of a DataFrame into a Cassandra table
x = sc.parallelize([(3, 'Mary', 'Johnson', ['Mary1@gmail.com', 'Mary2@yahoo.com'])])
x1 = spark.createDataFrame(x, schema = ['id', 'firstname', 'lastname', 'emails'])
x1.write.format("org.apache.spark.sql.cassandra").options(table="student", keyspace="classroom").mode("append").save()
people = spark.read.format("org.apache.spark.sql.cassandra").options(table="student", keyspace="classroom").load()
people.show()

