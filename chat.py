
import threading
import json
import kafka  # pip3 install kafka-python
import dotenv  # pip3 install python-dotenv

"""
https://kafka-python.readthedocs.io/en/master/usage.html
"""

config = dotenv.dotenv_values()

def producer_thread():
    producer = kafka.KafkaProducer(
        sasl_mechanism='PLAIN',
        sasl_plain_username=config['SASL_USERNAME'],
        sasl_plain_password=config['SASL_PASSWORD'],
        security_protocol='SASL_SSL',
        bootstrap_servers=config['BOOTSTRAP_SERVERS'])
    print('producer running')
    while True:
        print('> ', end = '')
        text = input()
        producer.send('chat', json.dumps({'name': 'Tom', 'message': text}).encode()).get(timeout=1.0)
        producer.flush()

def consumer_thread():
    consumer = kafka.KafkaConsumer('chat',
        group_id='chat_py',
        enable_auto_commit=False,
        sasl_mechanism='PLAIN',
        sasl_plain_username=config['SASL_USERNAME'],
        sasl_plain_password=config['SASL_PASSWORD'],
        security_protocol='SASL_SSL',
        bootstrap_servers=config['BOOTSTRAP_SERVERS'])
    print('consumer running')
    for message in consumer:
        consumer.commit()
        print(message)
        print(message.value.decode())

t = threading.Thread(target=consumer_thread)
t.start()
producer_thread()
