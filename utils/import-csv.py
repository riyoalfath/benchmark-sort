import csv
import mysql.connector

# Koneksi
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="a",
    database="benchmark_sort"
)
cursor = conn.cursor()

# Buat tabel (sekali saja)
cursor.execute("""
CREATE TABLE IF NOT EXISTS customers (
    invoice_no VARCHAR(20),
    customer_id VARCHAR(20),
    gender VARCHAR(10),
    age INT,
    category VARCHAR(50),
    quantity INT,
    price FLOAT,
    payment_method VARCHAR(50),
    invoice_date VARCHAR(20),
    shopping_mall VARCHAR(100)
)
""")

# Insert data
with open("../data/customer_shopping_data.csv", newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # skip header

    for row in reader:
        cursor.execute("""
            INSERT INTO customers 
            (invoice_no, customer_id, gender, age, category, quantity, price, 
             payment_method, invoice_date, shopping_mall)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, row)

conn.commit()
cursor.close()
conn.close()
