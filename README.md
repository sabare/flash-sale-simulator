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

The exact application framework will be selected before milestone 1.
