from kafka import KafkaConsumer

# Set Configuration parameters
KAFKA_BROKERS = 'broker1_a.hostname:9093,broker2_a.hostname:9093,broker3_a.hostname:9093,broker4_a.hostname:9093,broker5_a.hostname:9093'
TOPIC_NAME = 'dual_ingest_topic'

# Specify the output file path
OUTPUT_FILE = '/tmp/kafka_messages.txt'

# Specify your desired consumer group ID
CONSUMER_GROUP_ID = 'dual_ingest_consumer'
CERTFILE = '/opt/certs/cert.pem'

# Set gap between consumer cycles
SLEEP = 5

def write_to_file(message):
    with open(OUTPUT_FILE, 'a') as file:
        file.write(f"{message}\n")
        file.close()

# Set up Kafka consumer
consumer = KafkaConsumer(TOPIC_NAME,
                              bootstrap_servers=KAFKA_BROKERS,
                              security_protocol='SASL_SSL',
                              #auto_offset_reset='earliest',
                              enable_auto_commit='true',
                              auto_commit_interval_ms='1000',
                              ssl_check_hostname=True,
                              ssl_cafile=CERTFILE,
                              sasl_mechanism = 'GSSAPI',
                              group_id='group_1')

# Continuously consume messages from the topic and print them to the console
for message in consumer:
    print(message.value.decode('utf-8'))
    write_to_file(message.value.decode('utf-8'))

# Close consumer
consumer.close()
