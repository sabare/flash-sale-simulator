# Flash Sale Simulator

A backend-focused simulation of a high-demand flash sale. The goal is to show
how a system can accept a very large burst of purchase attempts while selling
only the available inventory, without overwhelming its primary database.

## The problem

Imagine one million customers trying to buy a product with only 100 units in
stock. The system must:

- never oversell;
- prevent one user from claiming the product twice;
- return a fast response when stock is gone;
- tolerate client retries without creating duplicate orders; and
- continue processing accepted orders even if downstream services are slow.

## Target architecture

```text
Client -> Load balancer -> Stateless API -> Redis atomic reservation
                                           |
                                           v
                                      Order queue -> Worker -> Database
```

Redis acts as the fast, atomic inventory gate. Only successful reservations
are placed on the queue; workers persist them as orders asynchronously. This
keeps the database out of the hot path during the traffic spike.

## Build milestones

1. Create a small API with a health endpoint and a sale/product model.
2. Add atomic inventory reservation in Redis.
3. Add one-purchase-per-user and idempotency protection.
4. Add an asynchronous order worker and durable order storage.
5. Add rate limiting, reservation expiry, metrics, and load tests.

## Core invariant

For a product, successful reservations must never exceed its configured stock.

## Local services (planned)

- API service
- Redis
- PostgreSQL
- Background order worker

The MVP API uses Python and FastAPI.

## Run the first milestone

This MVP uses Python 3.11+ and FastAPI.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

In another terminal, verify the service:

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/products/limited-headphones
pytest
```

## Current API

Reserve one item with a user identifier:

```bash
curl -X POST http://127.0.0.1:8000/sales/limited-headphones/reservations \
  -H "content-type: application/json" \
  -d '{"user_id":"user-123"}'
```

The endpoint returns `201` while stock remains and `409` when it is sold out.
The stock counter lives in Redis and is decremented by a Lua script, so the
check-and-decrement is atomic even when multiple API instances receive traffic.
If Redis is unavailable, the API returns `503` instead of risking an
incorrect sale.

## Run with Docker

Docker Compose starts the API and Redis together. Redis is present now so the
next milestone can add atomic inventory reservations without changing the
local environment.

```bash
docker compose up --build
```

Stop the services with:

```bash
docker compose down
```
