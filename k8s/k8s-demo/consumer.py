from confluent_kafka import Consumer, KafkaError
import json

conf = {
    'bootstrap.servers': 'localhost:9094',
    'group.id': 'group-consumer',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
topic = 'demo_topic'

consumer.subscribe([topic])

print("Start listening")

try:
    while True:
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        
        val = json.loads(msg.value().decode("utf-8"))
        print(f"Received {val['status']}, money {val['amount']}")
except KeyboardInterrupt:
    print("Stop consume")
finally:
    consumer.close()