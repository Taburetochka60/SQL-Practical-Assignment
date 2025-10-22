USE ecommerce_db;

CREATE INDEX idx_users_reg_date_id ON users (registration_date, id);

CREATE INDEX idx_products_category_id_price ON products (category, id, price);

CREATE INDEX idx_orders_user_product_qty ON orders (user_id, product_id, quantity);

explain analyze SELECT 
    u.id,
    u.name,
    SUM(o.quantity * p.price) AS total_spent
FROM users u
JOIN orders o 
    ON o.user_id = u.id
JOIN products p 
    ON p.id = o.product_id
WHERE 
    u.registration_date > '2024-01-01'
    AND p.category = 'Electronics'
GROUP BY 
    u.id, u.name
ORDER BY 
    total_spent DESC;



