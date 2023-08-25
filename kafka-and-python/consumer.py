from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'aula-bigdata',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     group_id='group1',
     value_deserializer=lambda x: loads(x.decode('utf-8')));

for message in consumer:
    message = message.value;
    print('{}'.format(message))