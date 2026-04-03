-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    status VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Order items table
CREATE TABLE IF NOT EXISTS order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INT,
    sku VARCHAR,
    quantity INT
);

-- Seed baseline data for orders DB initialization checks
INSERT INTO orders (order_id, status, created_at)
VALUES
    (1, 'CONFIRMED', CURRENT_TIMESTAMP - INTERVAL '2 days'),
    (2, 'CONFIRMED', CURRENT_TIMESTAMP - INTERVAL '1 day'),
    (3, 'PENDING', CURRENT_TIMESTAMP)
ON CONFLICT (order_id) DO NOTHING;

INSERT INTO order_items (item_id, order_id, sku, quantity)
VALUES
    (1, 1, 'SKU001', 1),
    (2, 1, 'SKU002', 2),
    (3, 2, 'SKU010', 1),
    (4, 3, 'SKU003', 1)
ON CONFLICT (item_id) DO NOTHING;

SELECT setval('orders_order_id_seq', COALESCE((SELECT MAX(order_id) FROM orders), 1), true);
SELECT setval('order_items_item_id_seq', COALESCE((SELECT MAX(item_id) FROM order_items), 1), true);