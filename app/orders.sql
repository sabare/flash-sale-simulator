CREATE TABLE IF NOT EXISTS orders (
    reservation_id UUID PRIMARY KEY,
    product_id TEXT NOT NULL CHECK (length(product_id) > 0),
    user_id TEXT NOT NULL CHECK (length(user_id) BETWEEN 1 AND 100),
    status TEXT NOT NULL DEFAULT 'pending_payment'
        CHECK (status IN ('pending_payment', 'paid', 'cancelled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (product_id, user_id)
);
