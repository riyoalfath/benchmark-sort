from flask import Flask, render_template, jsonify, request
import mysql.connector
import subprocess
import json
import os
import csv

app = Flask(__name__)

def get_mysql_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="a",
        database="benchmark_sort"
    )

CPP_EXECUTABLE = './merge_sort'
DATA_CSV = './data/customer_shopping_data.csv'

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/all')
def all():
    rows = []

    with open(DATA_CSV, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)

    return jsonify({
        "status": "success",
        "count": len(rows),
        "data": rows
    })

@app.route('/cpp-sort')
def cpp_sort():
    # ambil parameter sort field
    sort_field = request.args.get("field")

    # jalankan C++ dengan argumen sort_field
    result = subprocess.run(
        [CPP_EXECUTABLE, sort_field],
        capture_output=True,
        text=True
    )

    output = result.stdout

    # parsing output JSON
    start = output.find("---START_JSON---") + len("---START_JSON---")
    end = output.find("---END_JSON---")
    
    if start == -1 or end == -1:
        return jsonify({"status": "error", "message": "Invalid JSON output"}), 500

    json_text = output[start:end].strip()

    data = json.loads(json_text)

    return jsonify({
        "status": "success",
        "duration": data["duration"],
        "data": data["data"]
    })

@app.route('/mysql-sort')
def mysql_sort():
    sort_field = request.args.get("field")

    # Daftar kolom yang diizinkan agar aman dari SQL injection
    allowed_fields = {
        "invoice_no",
        "customer_id",
        "gender",
        "age",
        "category",
        "quantity",
        "price",
        "payment_method",
        "invoice_date",
        "shopping_mall"
    }

    if sort_field not in allowed_fields:
        return jsonify({
            "status": "error",
            "message": f"Invalid sort field: {sort_field}"
        }), 400

    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(dictionary=True)

        query = f"SELECT * FROM customers ORDER BY {sort_field} ASC"
        cursor.execute(query)
        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        return jsonify({
            "status": "success",
            "count": len(rows),
            "data": rows
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500



if __name__ == '__main__':
    
    app.run(debug=True, port=5000)