from fastapi import FastAPI
from typing import List
from models import Notification
from aiokafka import AIOKafkaConsumer
from contextlib import asynccontextmanager
import asyncio, json

PRODUCT_NOT_FOUND_TOPIC = "product_not_found_events"
OUT_OF_STOCK_TOPIC = "out_of_stock_events"

@asynccontextmanager
async def lifespan(app: FastAPI):
    confirmed_consumer = AIOKafkaConsumer(
        "order-confirmed",
        bootstrap_servers='kafka:9092',
        group_id="notifications-confirmed-group",
        auto_offset_reset="earliest"
    )

    product_not_found_consumer = AIOKafkaConsumer(
        PRODUCT_NOT_FOUND_TOPIC,
        bootstrap_servers='kafka:9092',
        group_id="notifications-product-not-found-group",
        auto_offset_reset="earliest"
    )

    out_of_stock_consumer = AIOKafkaConsumer(
        OUT_OF_STOCK_TOPIC,
        bootstrap_servers='kafka:9092',
        group_id="notifications-out-of-stock-group",
        auto_offset_reset="earliest"
    )

    await confirmed_consumer.start()
    await product_not_found_consumer.start()
    await out_of_stock_consumer.start()

    task1 = asyncio.create_task(consume_confirmed(confirmed_consumer))
    task2 = asyncio.create_task(consume_error(product_not_found_consumer))
    task3 = asyncio.create_task(consume_error(out_of_stock_consumer))

    yield

    task1.cancel()
    task2.cancel()
    task3.cancel()

    await confirmed_consumer.stop()
    await product_not_found_consumer.stop()
    await out_of_stock_consumer.stop()

app = FastAPI(title="Notifications Service", lifespan=lifespan)

notifications_db: List[Notification] = []

async def consume_confirmed(consumer: AIOKafkaConsumer):
    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode('utf-8'))

            notification = Notification(
                order_id=data['order_id'],
                product_id=data['product_id'],
                message=f"Order {data['order_id']} for product {data['product_id']} has been placed."
            )

            notifications_db.append(notification)
            print(f"[SUCCESS NOTIFICATION] {notification.message}")

    except asyncio.CancelledError:
        pass

async def consume_error(consumer: AIOKafkaConsumer):
    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode('utf-8'))

            notification = Notification(
                order_id=data['order_id'],
                product_id=data['product_id'],
                message=(
                    f"Narudžbina {data['order_id']} je odbijena. "
                    f"Proizvod: {data['product_id']}. "
                    f"Razlog: {data['error_reason']}."
                )
            )

            notifications_db.append(notification)

            print(
                f"[ERROR NOTIFICATION] Narudžbina {data['order_id']} je odbijena. "
                f"Razlog: {data['error_reason']}"
            )

    except asyncio.CancelledError:
        pass

@app.get("/notifications", response_model=List[Notification])
def get_notifications():
    return notifications_db