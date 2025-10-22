USE ecommerce_db;

drop  index idx_users_reg_date_id on users;
drop  index idx_products_category_id_price on products;
drop  index idx_orders_user_product_qty on orders;


explain analyze select 
    u.id,
    u.name,
    (
        SELECT 
            SUM(
                (SELECT o2.quantity 
                 FROM orders o2 
                 WHERE o2.id = o.id) *
                (SELECT p2.price 
                 FROM products p2 
                 WHERE p2.id = o.product_id)
            )
        FROM orders o
        WHERE o.user_id IN (
            SELECT u2.id 
            FROM users u2 
            WHERE u2.id = u.id
        )
        AND o.product_id IN (
            SELECT p3.id 
            FROM products p3 
            WHERE p3.category IN (
                SELECT p4.category  
                FROM products p4 
                WHERE p4.id = o.product_id
            )
            AND p3.category = 'Electronics'
        )
    ) AS total_spent
FROM users u
WHERE u.registration_date > '2024-01-01'
ORDER BY total_spent DESC;