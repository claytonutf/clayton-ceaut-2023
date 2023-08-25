from kafka import KafkaProducer
import json

'''
EXEMPLO 1
'''
#producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
## Asynchronous by default
#producer.send('aula-bigdata', b'envio de bytes')
#producer.close()

'''
EXEMPLO 2
'''
#producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'), bootstrap_servers="localhost:9092")
#event = {
#    "brand": "ford",
#    "model": "mustang",
#    "color": "blue"
#}
#future = producer.send("aula-bigdata", event)
#producer.close()