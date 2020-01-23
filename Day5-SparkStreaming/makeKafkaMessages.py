# pip install kafka-python
from random import randint
from json import dumps
from time import sleep
from kafka import KafkaProducer
import string
 
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

def randomname():
   length = randint(3, 10)
   ret = string.ascii_uppercase[randint(0,25)]
   for x in range(randint(3,10)):
      ret += string.ascii_lowercase[randint(0,25)]
   return ret

for e in range(1000):
    data = {'amount' : randint(100,10000), 'id' : e, 'name':randomname(), 'parentid': randint(1,4)}
    print(data)
    producer.send('classroom', value=data)
    sleep(2)
