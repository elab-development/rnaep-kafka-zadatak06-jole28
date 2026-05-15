from fastapi import FastAPI, HTTPException
from typing import List
from aiokafka import AIOKafkaProducer
from contextlib import asynccontextmanager
from models import Order

producer = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer

    producer = AIOKafkaProducer(bootstrap_servers='kafka:9092')
    await producer.start()

    yield

    await producer.stop()

app = FastAPI(title="Orders Service", lifespan=lifespan)

orders_db: List[Order] = []

@app.get("/orders")
def get_orders():
    return orders_db

@app.post("/orders", response_model=Order)
async def create_order(order: Order):
    try:
        await producer.send_and_wait(
            "order-created",
            order.model_dump_json().encode('utf-8')
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    orders_db.append(order)
    return order