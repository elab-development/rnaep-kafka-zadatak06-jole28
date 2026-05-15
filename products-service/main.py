from fastapi import FastAPI
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from contextlib import asynccontextmanager
from models import Product
import asyncio, json
from datetime import datetime

producer = None

PRODUCT_NOT_FOUND_TOPIC = "product_not_found_events"
OUT_OF_STOCK_TOPIC = "out_of_stock_events"

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer

    producer = AIOKafkaProducer(bootstrap_servers='kafka:9092')
    await producer.start()

    consumer = AIOKafkaConsumer(
        "order-created",
        bootstrap_servers='kafka:9092',
        group_id="products-group",
        auto_offset_reset="earliest"
    )

    await consumer.start()
    task = asyncio.create_task(consume(consumer))

    yield

    task.cancel()
    await consumer.stop()
    await producer.stop()

app = FastAPI(title="Products Service", lifespan=lifespan)

products_db = {
    1: Product(id=1, name="Laptop", price=1500.0, quantity=10),
    2: Product(id=2, name="Mouse", price=25.0, quantity=50)
}

async def send_error_event(topic: str, order_id: int, product_id: int, error_reason: str):
    event = {
        "order_id": order_id,
        "product_id": product_id,
        "timestamp": datetime.utcnow().isoformat(),
        "error_reason": error_reason
    }

    await producer.send_and_wait(
        topic,
        json.dumps(event).encode('utf-8')
    )

async def consume(consumer: AIOKafkaConsumer):
    try:
        async for msg in consumer:
            order = json.loads(msg.value.decode('utf-8'))

            order_id = order['id']
            product_id = order['product_id']
            quantity = order['quantity']

            product = products_db.get(product_id)

            if product is None:
                await send_error_event(
                    PRODUCT_NOT_FOUND_TOPIC,
                    order_id,
                    product_id,
                    "Proizvod ne postoji u katalogu"
                )
                continue

            if product.quantity < quantity:
                await send_error_event(
                    OUT_OF_STOCK_TOPIC,
                    order_id,
                    product_id,
                    "Nedovoljna količina na stanju"
                )
                continue

            product.quantity -= quantity

            await producer.send_and_wait(
                "order-confirmed",
                json.dumps({
                    "order_id": order_id,
                    "product_id": product.id
                }).encode('utf-8')
            )

    except asyncio.CancelledError:
        pass

@app.get("/products")
def get_products():
    return products_db