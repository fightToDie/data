from kafka import KafkaConsumer
from json import loads
import configparser

properties = configparser.ConfigParser()
properties.read('config.ini')
broker = properties.get('CONFIG', 'BROKER')
topicname = properties.get('CONFIG', 'TOPIC')

consumer = KafkaConsumer(
    topicname,
    bootstrap_servers = [broker],
    auto_offset_reset = 'earliest',
    enable_auto_commit = True,
    group_id = 'my-group',
    value_deserializer = lambda x: loads(x.decode('utf-8')),
    consumer_timeout_ms = 1000
)

print('[begin] get consumer list')

for message in consumer:
    print("Topic: %s, Partition: %d, Offset: %d, Key = %s, Value = %s"
      % (
    message.topic, message.partition, message.offset, message.key, message.value
      ))

print('[end] get consumer list')
