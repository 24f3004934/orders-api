
import base64
import hashlib
import json
import math
import os
import time
import uuid

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from upstash_redis import Redis


# ---------------------------------------
# Configuration
# ---------------------------------------

TOTAL_ORDERS = 52
RATE_LIMIT = 17
WINDOW_SECONDS = 10

app = FastAPI(
    title="Orders API",
    version="1.0.0",
)

# ---------------------------------------
# Redis Connection
# ---------------------------------------

from pathlib import Path
from dotenv import load_dotenv

load_dotenv(
    dotenv_path=Path(__file__).resolve().parent / ".env",
    override=True,
)

redis_url = os.getenv("UPSTASH_REDIS_REST_URL")
redis_token = os.getenv("UPSTASH_REDIS_REST_TOKEN")

if not redis_url or not redis_token:
    raise RuntimeError(
        "Missing UPSTASH_REDIS_REST_URL or "
        "UPSTASH_REDIS_REST_TOKEN environment variable"
    )

db = Redis(
    url=redis_url,
    token=redis_token,
)

# ---------------------------------------
# CORS
# ---------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After"],
)

# ---------------------------------------
# Fixed Catalog
# ---------------------------------------

ORDERS = [
    {
        "id": i,
        "product": f"Product {i}",
        "quantity": 1,
    }
    for i in range(1, TOTAL_ORDERS + 1)
]

# ---------------------------------------
# Request Model
# ---------------------------------------

class OrderRequest(BaseModel):
    product: str = "Sample Product"
    quantity: int = Field(default=1, ge=1)


# ---------------------------------------
# Atomic Redis Rate Limiting
# ---------------------------------------

RATE_SCRIPT = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local request_id = ARGV[4]

redis.call(
    'ZREMRANGEBYSCORE',
    key,
    '-inf',
    now - window
)

local count = redis.call('ZCARD', key)

if count >= limit then
    local oldest = redis.call(
        'ZRANGE',
        key,
        0,
        0,
        'WITHSCORES'
    )

    local retry = math.max(
        1,
        math.ceil(
            (tonumber(oldest[2]) + window - now) / 1000
        )
    )

    return {0, retry}
end

redis.call(
    'ZADD',
    key,
    now,
    request_id
)

redis.call('PEXPIRE', key, window)

return {1, 0}
"""


@app.middleware("http")
async def rate_limit_middleware(
    request: Request,
    call_next,
):
    # CORS preflight requests are not counted.
    if request.method == "OPTIONS":
        return await call_next(request)

    client_id = request.headers.get(
        "X-Client-Id",
        "default",
    )

    client_hash = hashlib.sha256(
        client_id.encode()
    ).hexdigest()

    redis_key = f"orders:rate:{client_hash}"

    now = int(time.time() * 1000)

    result = db.eval(
        RATE_SCRIPT,
        keys=[redis_key],
        args=[
            now,
            WINDOW_SECONDS * 1000,
            RATE_LIMIT,
            str(uuid.uuid4()),
        ],
    )

    allowed = int(result[0])
    retry_after = int(result[1])

    if not allowed:
        return JSONResponse(
            status_code=429,
            content={
                "detail": "Rate limit exceeded"
            },
            headers={
                "Retry-After": str(retry_after)
            },
        )

    return await call_next(request)


# ---------------------------------------
# Idempotent Order Creation
# ---------------------------------------

@app.post("/orders", status_code=201)
def create_order(
    order: OrderRequest,
    idempotency_key: str = Header(
        ...,
        alias="Idempotency-Key",
    ),
):
    key_hash = hashlib.sha256(
        idempotency_key.encode()
    ).hexdigest()

    redis_key = f"orders:idempotency:{key_hash}"

    new_order = {
        "id": str(uuid.uuid4()),
        "product": order.product,
        "quantity": order.quantity,
    }

    # Only the first request can create the record.
    created = db.set(
        redis_key,
        json.dumps(new_order),
        nx=True,
    )

    # Read the stored result for both new and
    # repeated requests.
    stored = db.get(redis_key)

    if stored is None:
        raise HTTPException(
            status_code=503,
            detail="Unable to retrieve order",
        )

    return json.loads(stored)


# ---------------------------------------
# Cursor Encoding
# ---------------------------------------

def encode_cursor(position: int) -> str:
    return base64.urlsafe_b64encode(
        str(position).encode("utf-8")
    ).decode("utf-8")


def decode_cursor(cursor: str) -> int:
    try:
        decoded = base64.urlsafe_b64decode(
            cursor.encode("utf-8")
        ).decode("utf-8")

        position = int(decoded)

        if position < 0 or position > TOTAL_ORDERS:
            raise ValueError("Invalid position")

        return position

    except Exception:
        raise HTTPException(
            status_code=400,
            detail="Invalid cursor",
        )


# ---------------------------------------
# Cursor-Based Pagination
# ---------------------------------------

@app.get("/orders")
def list_orders(
    limit: int = Query(
        default=10,
        ge=1,
    ),
    cursor: str | None = None,
):
    start = (
        decode_cursor(cursor)
        if cursor
        else 0
    )

    end = min(
        start + limit,
        TOTAL_ORDERS,
    )

    items = ORDERS[start:end]

    next_cursor = (
        encode_cursor(end)
        if end < TOTAL_ORDERS
        else None
    )

    return {
        "items": items,
        "next_cursor": next_cursor,
    }


# ---------------------------------------
# Health Check
# ---------------------------------------

@app.get("/")
def home():
    return {
        "status": "ok",
        "service": "Orders API",
        "total_orders": TOTAL_ORDERS,
        "rate_limit": RATE_LIMIT,
        "window_seconds": WINDOW_SECONDS,
    }
