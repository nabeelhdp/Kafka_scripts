from kafka import KafkaProducer
import time
import argparse

# Set your Kafka broker details
CLUSTER_A = 'broker1_a.hostname:9093,broker2_a.hostname:9093,broker3_a.hostname:9093,broker4_a.hostname:9093,broker5_a.hostname:9093'
CLUSTER_B = 'broker1_b.hostname:9093,broker2_b.hostname:9093,broker3_b.hostname:9093,broker4_b.hostname:9093,broker5_b.hostname:9093'
TOPIC_NAME = 'dual_ingest_topic'
CERTFILE = '/opt/certs/cert.pem'
# Replace with your actual log file path from where you pick the messages to send via the producer
LOG_FILE_PATH = '/opt/test/kafka-message-feed-input.txt'  

def parse_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['dual', 'srm'], default='srm', 
                        help='Defaults to SRM, else choose dual or srm')
    args = parser.parse_args()
    return args.mode


def tail_log_file(log_file_path,script_mode):
    try:
        producer_A = KafkaProducer(bootstrap_servers=CLUSTER_A,
                          security_protocol='SASL_SSL',
                          ssl_check_hostname=True,
                          ssl_cafile=CERTFILE,
                          sasl_mechanism = 'GSSAPI')

        producer_B = ""
        if script_mode == "dual":
            producer_B = KafkaProducer(bootstrap_servers=CLUSTER_B,
                              security_protocol='SASL_SSL',
                              ssl_check_hostname=True,
                              ssl_cafile=CERTFILE,
                              sasl_mechanism = 'GSSAPI')

        with open(log_file_path, 'r') as file:
            # Read the entire file initially
            lines = file.readlines()
            while True:
                # Check for new lines
                new_lines = file.readlines()
                if new_lines:
                    lines.extend(new_lines)
                    for line in new_lines:
                        # Send each new line to Kafka topic
                        producer_A.send(TOPIC_NAME, line.strip().encode('utf-8'))
                        producer_A.flush()
                        if script_mode == "dual":
                            producer_B.send(TOPIC_NAME, line.strip().encode('utf-8'))
                            producer_B.flush()
 
                # Wait for a short interval before checking again
                time.sleep(1)
    except FileNotFoundError:
        print(f"File '{log_file_path}' not found.")
    except Exception as e:
        print(f"Error reading file: {e}")

if __name__ == '__main__':
    script_mode = parse_arg()
    tail_log_file(LOG_FILE_PATH,script_mode)
