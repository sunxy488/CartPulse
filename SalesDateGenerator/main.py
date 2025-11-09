import json
import os
import random
import time
from datetime import datetime, timezone
from uuid import uuid4

from faker import Faker
from confluent_kafka import SerializingProducer

fake = Faker()

# ================== Configs ==================
TOPIC = "financial_transactions"
BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")

# precise rate control (messages per second)
TARGET_RPS = int(os.getenv("TARGET_RPS", "100"))

EVENT_WEIGHTS = {"page_view": 0.5, "add_to_cart": 0.3, "checkout": 0.2}
CURRENCIES = ["USD", "GBP"]
CATEGORIES = ["electronic", "fashion", "grocery", "home", "beauty", "sports"]
PRODUCTS = [
    ("product1", "laptop", "apple"),
    ("product2", "mobile", "samsung"),
    ("product3", "tablet", "oneplus"),
    ("product4", "watch", "mi"),
    ("product5", "headphone", "sony"),
    ("product6", "speaker", "boat"),
]

# Pre-generate users to avoid slow Faker calls in the hot path
USERS = [fake.user_name() for _ in range(5000)]

# Keep a small in-memory open carts map to naturally generate abandoned carts
open_carts = {}  # cartId -> {"userId": ..., "items": [...], "createdAt": ...}


# =============================================


# ------------- Helpers -------------
def now_iso() -> str:
    """Return current UTC time in ISO8601 format (Kibana/Flink friendly)."""
    return datetime.now(timezone.utc).isoformat()


def pick_user() -> str:
    """Return a pseudo userId from pre-generated pool (fast)."""
    return random.choice(USERS)


def pick_product() -> dict:
    """Return one product item; keep math simple to reduce overhead."""
    pid, name, brand = random.choice(PRODUCTS)
    category = random.choice(CATEGORIES)
    # Keep uniform+round but avoid excessive precision work
    price = round(random.random() * 990 + 10, 2)  # 10..1000
    qty = random.randint(1, 5)
    return {
        "productId": pid,
        "productName": name,
        "productCategory": category,
        "productBrand": brand,
        "productPrice": price,
        "productQuantity": qty,
        "totalAmount": round(price * qty, 2),
    }


# -----------------------------------


# ------------- Event generators -------------
def generate_page_view() -> dict:
    return {
        "eventId": str(uuid4()),
        "eventType": "page_view",
        "timestamp": now_iso(),
        "userId": pick_user(),
        "page": random.choice(["/home", "/plp", "/pdp", "/cart", "/checkout"]),
        "referrer": random.choice(["direct", "email", "ads", "social"]),
        "sessionId": str(uuid4()),
    }


def generate_add_to_cart() -> dict:
    user = pick_user()
    cart_id = str(uuid4())
    item = pick_product()
    evt = {
        "eventId": str(uuid4()),
        "eventType": "add_to_cart",
        "timestamp": now_iso(),
        "userId": user,
        "cartId": cart_id,
        "sessionId": str(uuid4()),
        "currency": random.choice(CURRENCIES),
        **item,
    }
    open_carts[cart_id] = {
        "userId": user,
        "items": [item],
        "createdAt": datetime.now(timezone.utc),
    }
    return evt


def maybe_checkout_from_open_cart() -> dict | None:
    if not open_carts or random.random() < 0.5:
        return None
    cart_id, cart_state = random.choice(list(open_carts.items()))
    total = 0.0
    for i in cart_state["items"]:
        total += i["productPrice"] * i["productQuantity"]
    evt = {
        "eventId": str(uuid4()),
        "eventType": "checkout",
        "timestamp": now_iso(),
        "userId": cart_state["userId"],
        "cartId": cart_id,
        "sessionId": str(uuid4()),
        "currency": random.choice(CURRENCIES),
        "transactionId": str(uuid4()),
        "paymentMethod": random.choice(["credit_card", "debit_card", "online_transfer"]),
        "totalAmount": round(total, 2),
    }
    open_carts.pop(cart_id, None)
    return evt


def generate_checkout_direct() -> dict:
    item = pick_product()
    return {
        "eventId": str(uuid4()),
        "eventType": "checkout",
        "timestamp": now_iso(),
        "userId": pick_user(),
        "cartId": str(uuid4()),
        "sessionId": str(uuid4()),
        "currency": random.choice(CURRENCIES),
        "transactionId": str(uuid4()),
        "paymentMethod": random.choice(["credit_card", "debit_card", "online_transfer"]),
        **item,
    }


def generate_event() -> dict:
    event_type = random.choices(
        population=("page_view", "add_to_cart", "checkout"),
        weights=(EVENT_WEIGHTS["page_view"], EVENT_WEIGHTS["add_to_cart"], EVENT_WEIGHTS["checkout"]),
        k=1
    )[0]
    if event_type == "page_view":
        return generate_page_view()
    elif event_type == "add_to_cart":
        return generate_add_to_cart()
    evt = maybe_checkout_from_open_cart()
    return evt if evt is not None else generate_checkout_direct()


# --------------------------------------------


# ------------- Kafka producer -------------
def build_producer() -> SerializingProducer:
    """Create a tuned Kafka producer for higher throughput."""
    return SerializingProducer({
        "bootstrap.servers": BOOTSTRAP,
        "linger.ms": 5,  # small batch wait improves throughput
        "batch.size": 65536,  # 64KB; adjust as needed
        "compression.type": "lz4",  # good balance of speed/ratio
        "acks": "1",
    })


# -----------------------------------------


def main():
    producer = build_producer()

    # precise pacing
    period = 1.0 / TARGET_RPS
    next_ts = time.perf_counter()

    sent = 0
    win_sent = 0
    win_start = time.perf_counter()

    try:
        while True:
            event = generate_event()
            key = event.get("userId") or event.get("cartId") or event["eventId"]

            # compact JSON to reduce serialization overhead
            payload = json.dumps(event, separators=(",", ":"))

            producer.produce(TOPIC, key=key, value=payload)

            sent += 1
            win_sent += 1

            # poll every 200 messages instead of every message
            if (sent % 200) == 0:
                producer.poll(0)

            # print throughput every ~5 seconds
            now = time.perf_counter()
            if now - win_start >= 5.0:
                print(f"[THROUGHPUT] ~{win_sent / 5.0:.1f} msg/s")
                win_sent = 0
                win_start = now

            # adaptive sleep to hit target rps
            next_ts += period
            sleep_left = next_ts - time.perf_counter()
            if sleep_left > 0:
                time.sleep(sleep_left)
            else:
                # we're behind schedule; reset anchor to avoid drift
                next_ts = time.perf_counter()

    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user. Flushing...")
    except BufferError:
        print("[WARN] Buffer full! Backing off...")
        time.sleep(0.5)
    except Exception as e:
        print(f"[ERROR] {e}")
    finally:
        producer.flush(10)
        print("[INFO] Producer closed.")


if __name__ == "__main__":
    main()
