from cassandra.cluster import Cluster
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.execute('DROP KEYSPACE IF EXISTS classroom')
session.execute("CREATE KEYSPACE classroom WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':'1'}")
session = cluster.connect('classroom')
session.execute("create table People(PersonID int PRIMARY KEY, FirstName text)")
session.execute("insert into people (PersonID, Firstname) values (1, 'joey')")
session.execute("update people set firstname = 'Joey' where personid = 1")
session.execute("insert into people (personid, firstname) values (2, 'mike')")
session.execute("insert into people (personid, firstname) values (2, 'Mike')")
rows = session.execute('SELECT PersonID, FirstName FROM people')
print(list(rows))

# Python to access a Cassandra cluster through Spark
people = spark.read.format("org.apache.spark.sql.cassandra").options(table="people", keyspace="classroom").load()
people.show()

# Append the results of a DataFrame into a Cassandra table
x = sc.parallelize([(3, 'Jack')])
x1 = spark.createDataFrame(x, schema = ['personid', 'firstname'])
x1.write.format("org.apache.spark.sql.cassandra").options(table="people", keyspace="classroom").mode("append").save()
people = spark.read.format("org.apache.spark.sql.cassandra").options(table="people", keyspace="classroom").load()
people.show()

