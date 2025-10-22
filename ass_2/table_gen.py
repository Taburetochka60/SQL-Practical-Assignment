import mysql.connector
from faker import Faker
import random
from datetime import datetime
import time

# MySQL configuration
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'MySQL_Student123' 
}
DB_NAME = 'ecommerce_db'
ROW_COUNT = 5_000_000
BATCH_SIZE = 10_000  

fake = Faker()

def create_db_and_tables(cursor):
    """
    Creates the database and tables (users, products, orders).
    """
    try:
        print(f"Creating database '{DB_NAME}' (if not exists)...")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        cursor.execute(f"USE {DB_NAME}")

        print("Dropping old tables (if exist)...")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        cursor.execute("DROP TABLE IF EXISTS orders")
        cursor.execute("DROP TABLE IF EXISTS users")
        cursor.execute("DROP TABLE IF EXISTS products")

        print("Creating table 'users'...")
        cursor.execute("""
        CREATE TABLE users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100) NOT NULL UNIQUE,
            registration_date DATE NOT NULL
        )
        """)

        print("Creating table 'products'...")
        cursor.execute("""
        CREATE TABLE products (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            category VARCHAR(50),
            price DECIMAL(10, 2) NOT NULL
        )
        """)

        print("Creating table 'orders'...")
        cursor.execute("""
        CREATE TABLE orders (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            product_id INT NOT NULL,
            quantity INT NOT NULL,
            order_date DATE NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        )
        """)
        
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        print("Tables created successfully.")

    except mysql.connector.Error as err:
        print(f"Error creating tables: {err}")
        raise

def populate_users(cursor, conn, count):
    """
    Populates the 'users' table in batches.
    """
    print(f"Populating {count} users...")
    users_data = []
    query = "INSERT INTO users (name, email, registration_date) VALUES (%s, %s, %s)"
    
    start_time = time.time()
    for i in range(1, count + 1):
        name = fake.name()
        email = f'user{i}@datagen.com' 
        reg_date = fake.date_between(start_date='-5y', end_date='today')
        users_data.append((name, email, reg_date))

        if i % BATCH_SIZE == 0:
            cursor.executemany(query, users_data)
            conn.commit()
            users_data = [] 
            elapsed = time.time() - start_time
            print(f"  Inserted {i}/{count} users... (batch took {elapsed:.2f}s)")
            start_time = time.time()
    
    if users_data:
        cursor.executemany(query, users_data)
        conn.commit()
    print("Users table population completed.")

def populate_products(cursor, conn, count):
    """
    Populates the 'products' table in batches.
    """
    print(f"Populating {count} products...")
    products_data = []
    query = "INSERT INTO products (name, category, price) VALUES (%s, %s, %s)"
    categories = ['Electronics', 'Books', 'Clothing', 'Home', 'Grocery']
    
    start_time = time.time()
    for i in range(1, count + 1):
        name = f'Product {i} - {fake.word().capitalize()}'
        category = random.choice(categories)
        price = round(random.uniform(1.0, 1000.0), 2)
        products_data.append((name, category, price))

        if i % BATCH_SIZE == 0:
            cursor.executemany(query, products_data)
            conn.commit()
            products_data = []
            elapsed = time.time() - start_time
            print(f"  Inserted {i}/{count} products... (batch took {elapsed:.2f}s)")
            start_time = time.time()
            
    if products_data:
        cursor.executemany(query, products_data)
        conn.commit()
    print("Products table population completed.")

def populate_orders(cursor, conn, count):
    """
    Populates the 'orders' table in batches.
    Assumes user IDs and product IDs range from 1 to ROW_COUNT.
    """
    print(f"Populating {count} orders...")
    orders_data = []
    query = "INSERT INTO orders (user_id, product_id, quantity, order_date) VALUES (%s, %s, %s, %s)"
    
    start_time = time.time()
    for i in range(1, count + 1):
        user_id = random.randint(1, ROW_COUNT)
        product_id = random.randint(1, ROW_COUNT)
        quantity = random.randint(1, 10)
        order_date = fake.date_between(start_date='-5y', end_date='today')
        orders_data.append((user_id, product_id, quantity, order_date))

        if i % BATCH_SIZE == 0:
            cursor.executemany(query, orders_data)
            conn.commit()
            orders_data = []
            elapsed = time.time() - start_time
            print(f"  Inserted {i}/{count} orders... (batch took {elapsed:.2f}s)")
            start_time = time.time()

    if orders_data:
        cursor.executemany(query, orders_data)
        conn.commit()
    print("Orders table population completed.")


def main():
    """
    Main function: connects to MySQL, creates tables, and populates them.
    """
    conn = None
    cursor = None
    try:
        print("Connecting to MySQL...")
        # Connect without specifying DB to create it
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        script_start = time.time()
        
        # 1. Create schema
        create_db_and_tables(cursor)
        
        # 2. Disable checks for faster bulk insert
        print("Disabling UNIQUE and FOREIGN_KEY checks for bulk insert...")
        cursor.execute("SET foreign_key_checks = 0")
        cursor.execute("SET unique_checks = 0")
        conn.commit()

        # 3. Populate tables
        populate_users(cursor, conn, ROW_COUNT)
        populate_products(cursor, conn, ROW_COUNT)
        populate_orders(cursor, conn, ROW_COUNT)
        
        # 4. Enable checks back
        print("Enabling UNIQUE and FOREIGN_KEY checks...")
        cursor.execute("SET foreign_key_checks = 1")
        cursor.execute("SET unique_checks = 1")
        conn.commit()

        script_end = time.time()
        print("\n--- Database population completed successfully! ---")
        print(f"Total rows inserted (per table): {ROW_COUNT}")
        print(f"Total execution time: {script_end - script_start:.2f} seconds.")

    except mysql.connector.Error as err:
        print(f"MySQL error: {err}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("MySQL connection closed.")

if __name__ == "__main__":
    main()
