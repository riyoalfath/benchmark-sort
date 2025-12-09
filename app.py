from flask import Flask, render_template, jsonify, request
# import mysql.connector
import subprocess
import json
import os
import csv

app = Flask(__name__)

# def get_mysql_connection():
#     return mysql.connector.connect(
#         host="localhost",
#         user="root",
#         password="a",
#         database="benchmark_sort"
#     )

CPP_RADIX_EXECUTABLE = './bin/radix_sort.exe'
CPP_MERGE_SEQ_EXECUTABLE = './bin/merge_sort_seq.exe'
CPP_MERGE_EXECUTABLE = './bin/merge_sort.exe'
CPP_QUICK_EXECUTABLE = "./bin/quick_sort.exe"
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
    field = request.args.get("field")
    algo = request.args.get("algo")  # ⬅ ambil dari frontend

    if algo == "RADIX":
        exe = CPP_RADIX_EXECUTABLE
    elif algo == "MERGE":
        exe = CPP_MERGE_EXECUTABLE
    elif algo == "MERGE_SEQ":
        exe = CPP_MERGE_SEQ_EXECUTABLE
    elif algo == "QUICK":
        exe = CPP_QUICK_EXECUTABLE

    else:
        return jsonify({"status": "error", "message": "Unknown algo"}), 400

    result = subprocess.run([exe, field], capture_output=True, text=True)
    output = result.stdout

    start = output.find("---START_JSON---") + len("---START_JSON---")
    end = output.find("---END_JSON---")

    if start == -1 or end == -1:
        return jsonify({"status": "error", "message": "Invalid JSON output"}), 500

    data = json.loads(output[start:end].strip())

    return jsonify({
        "status": "success",
        "duration": data["duration"],
        "data": data["data"]
    })

# @app.route('/mysql-sort')
# def mysql_sort():
#     sort_field = request.args.get("field")

#     # Daftar kolom yang diizinkan agar aman dari SQL injection
#     allowed_fields = {
#         "invoice_no",
#         "customer_id",
#         "gender",
#         "age",
#         "category",
#         "quantity",
#         "price",
#         "payment_method",
#         "invoice_date",
#         "shopping_mall"
#     }

#     if sort_field not in allowed_fields:
#         return jsonify({
#             "status": "error",
#             "message": f"Invalid sort field: {sort_field}"
#         }), 400

#     try:
#         conn = get_mysql_connection()
#         cursor = conn.cursor(dictionary=True)

#         query = f"SELECT * FROM customers ORDER BY {sort_field} ASC"
#         cursor.execute(query)
#         rows = cursor.fetchall()

#         cursor.close()
#         conn.close()

#         return jsonify({
#             "status": "success",
#             "count": len(rows),
#             "data": rows
#         })

#     except Exception as e:
#         return jsonify({
#             "status": "error",
#             "message": str(e)
#         }), 500



if __name__ == '__main__':
    
    app.run(debug=True, port=5000)