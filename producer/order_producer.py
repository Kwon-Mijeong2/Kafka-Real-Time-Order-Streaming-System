import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = [
    "Laptop",
    "Mouse",
    "Keyboard",
    "Monitor",
    "Headphones",
    "Tablet"
]


def generate_order():
    return {
        "order_id": random.randint(10000, 99999),
        "user_id": random.randint(1, 1000),
        "product": random.choice(products),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(20, 2000), 2),
        "timestamp": datetime.now().isoformat()
    }


if __name__ == "__main__":
    while True:
        order = generate_order()

        producer.send("orders", order)

        print(f"Sent: {order}")

        time.sleep(2)