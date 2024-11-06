from kafka import KafkaProducer
from faker import Faker
import json
import time
import random

# Initialize Faker and Kafka producer
fake = Faker()
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Define topics
USER_ACTIVITY_TOPIC = 'user-activity'
TRANSACTION_TOPIC = 'transactions'

def generate_user_activity():
    return {
        'user_id': fake.uuid4(),
        'timestamp': fake.iso8601(),
        'action': random.choice(['view', 'click', 'add_to_cart', 'remove_from_cart']),
        'product_id': fake.uuid4(),
        'category': fake.word(),
        'ip_address': fake.ipv4()
    }

def generate_transaction():
    return {
        'transaction_id': fake.uuid4(),
        'user_id': fake.uuid4(),
        'timestamp': fake.iso8601(),
        'products': [
            {
                'product_id': fake.uuid4(),
                'quantity': random.randint(1, 5),
                'price': round(random.uniform(10, 1000), 2)
            } for _ in range(random.randint(1, 5))
        ],
        'total_amount': round(random.uniform(10, 5000), 2),
        'payment_method': random.choice(['credit_card', 'debit_card', 'paypal'])
    }

# Main loop to generate and send data
try:
    while True:
        # Generate and send user activity
        user_activity = generate_user_activity()
        producer.send(USER_ACTIVITY_TOPIC, user_activity)
        print(f"Sent user activity: {user_activity}")
        
        # Generate and send transaction (less frequently)
        if random.random() < 0.2:  # 20% chance of generating a transaction
            transaction = generate_transaction()
            producer.send(TRANSACTION_TOPIC, transaction)
            print(f"Sent transaction: {transaction}")
        
        # Sleep for a short interval
        time.sleep(random.uniform(0.1, 0.5))
except KeyboardInterrupt:
    print("Stopping data generation...")
finally:
    producer.close()